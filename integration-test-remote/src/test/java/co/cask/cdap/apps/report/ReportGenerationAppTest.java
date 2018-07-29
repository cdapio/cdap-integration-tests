/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.apps.report;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryService;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.report.proto.Filter;
import co.cask.cdap.report.proto.FilterCodec;
import co.cask.cdap.report.proto.ProgramRunStartMethod;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.Sort;
import co.cask.cdap.report.proto.ValueFilter;
import co.cask.cdap.report.proto.summary.ArtifactAggregate;
import co.cask.cdap.report.proto.summary.NamespaceAggregate;
import co.cask.cdap.report.proto.summary.ReportSummary;
import co.cask.cdap.report.proto.summary.StartMethodAggregate;
import co.cask.cdap.report.util.Constants;
import co.cask.cdap.report.util.ReportContentDeserializer;
import co.cask.cdap.report.util.ReportField;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReportGenerationAppTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationAppTest.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportContent.class, new ReportContentDeserializer())
    .registerTypeAdapter(Filter.class, new FilterCodec())
    .disableHtmlEscaping()
    .create();
  private static final ApplicationId REPORT_APP_ID = NamespaceId.SYSTEM.app("ReportGenerationApp");

  private static final ProgramId REPORT_SPARK_ID = REPORT_APP_ID.spark("ReportGenerationSpark");
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type REPORT_GEN_INFO_TYPE = new TypeToken<ReportGenerationInfo>() { }.getType();
  private static final Type REPORT_CONTENT_TYPE = new TypeToken<ReportContent>() { }.getType();
  private static final String PROGRAM_STATUS_PROCESSED = "user." + Constants.Metrics.RECORDS_PROCESSED_METRIC;
  private static final String LATEST_SYNC_TIME_METRIC = "user." + Constants.Metrics.SYNC_INTERVAL_TIME_MILLIS_METRIC;

  @Test
  public void testGenerateApp() throws Exception {

    Map<String, String> reportAppTags =
      ImmutableMap.of(co.cask.cdap.common.conf.Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                      co.cask.cdap.common.conf.Constants.Metrics.Tag.APP, REPORT_APP_ID.getApplication());

    long previousProcessed = getMetricValue(reportAppTags, PROGRAM_STATUS_PROCESSED);

    // deploy report generation app
    String version = getMetaClient().getVersion().getVersion();
    ArtifactSummary appArtifactSummary =
      new ArtifactSummary("cdap-program-report", version, ArtifactScope.SYSTEM);
    ApplicationManager reportApp = getTestManager().deployApplication(REPORT_APP_ID,
                                                                      new AppRequest(appArtifactSummary));
    SparkManager reportSpark = reportApp.getSparkManager(REPORT_SPARK_ID.getProgram());
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("task.client.system.resources.memory", "1024");
    runtimeArgs.put("task.client.system.resources.reserved.memory.override", "512");

    reportSpark.start(runtimeArgs);
    reportSpark.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // start the service
    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(PurchaseHistoryService.SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // make sure the spark app has started to process program status records
    checkMetricAtLeast(reportAppTags, PROGRAM_STATUS_PROCESSED, previousProcessed + 1,
                       PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS);

    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);


    // at least 3 states starting->running->killed for PurchaseHistoryService program is expected to be processed
    checkMetricAtLeast(reportAppTags, PROGRAM_STATUS_PROCESSED, previousProcessed + 3,
                       PROGRAM_START_STOP_TIMEOUT_SECONDS);
    // verify sync time metric to make sure the changes are synced to file
    long syncTimeAtLeast = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
    checkMetricAtLeast(reportAppTags, LATEST_SYNC_TIME_METRIC, syncTimeAtLeast,
                       PROGRAM_START_STOP_TIMEOUT_SECONDS);

    URL reportURL = reportSpark.getServiceURL().toURI().resolve("reports/").toURL();
    // create a list of filters to filter program runs from the TEST_NAMESPACE and with duration < 500 seconds
    List<Filter> filters =
      ImmutableList.of(
        new ValueFilter<>(Constants.NAMESPACE, ImmutableSet.of(TEST_NAMESPACE.getNamespace()), null),
        new ValueFilter<>(Constants.STATUS, ImmutableSet.of(ProgramRunStatus.KILLED), null));
    long currentTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // create a report generation request starting from one hour before the current time to current time,
    // with all fields included
    ReportGenerationRequest request =
      new ReportGenerationRequest("test_report", currentTimeSecs - 3600, currentTimeSecs,
                                  new ArrayList<>(ReportField.FIELD_NAME_MAP.keySet()),
                                  ImmutableList.of(new Sort(Constants.DURATION, Sort.Order.DESCENDING)), filters);
    RESTClient restClient = getRestClient();
    HttpResponse response = restClient.execute(HttpRequest.post(reportURL).withBody(GSON.toJson(request)).build(),
                                               getClientConfig().getAccessToken());

    Assert.assertEquals(200, response.getResponseCode());

    Map<String, String> reportIdMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_STRING_MAP);
    // get the report ID from response
    String reportId = reportIdMap.get("id");
    Assert.assertNotNull(reportId);
    URL reportInfoURL = reportURL.toURI().resolve("info?report-id=" + reportId).toURL();
    // wait for the report generation to complete
    Tasks.waitFor(ReportStatus.COMPLETED, () -> {
      ReportGenerationInfo reportGenerationInfo = getReportGenerationInfo(restClient, reportInfoURL);
      // fail the test if the report generation fails
      if (ReportStatus.FAILED.equals(reportGenerationInfo.getStatus())) {
        Assert.fail("Report generation failed");
      }
      return reportGenerationInfo.getStatus();
    }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS);

    ReportGenerationInfo reportGenerationInfo = getReportGenerationInfo(restClient, reportInfoURL);;
    // assert the summary content is expected
    ReportSummary summary = reportGenerationInfo.getSummary();
    Assert.assertNotNull(summary);
    Assert.assertEquals(ImmutableSet.of(new NamespaceAggregate(TEST_NAMESPACE.getNamespace(), 1)),
                        new HashSet<>(summary.getNamespaces()));
    Assert.assertEquals(ImmutableSet.of(new ArtifactAggregate("PurchaseApp", "1.0.0-SNAPSHOT", "USER", 1)),
                        new HashSet<>(summary.getArtifacts()));
    Assert.assertEquals(ImmutableSet.of(new StartMethodAggregate(ProgramRunStartMethod.MANUAL, 1)),
                        new HashSet<>(summary.getStartMethods()));
    // assert the number of report details is correct
    URL downloadReportURL = reportURL.toURI().resolve("download?report-id=" + reportId).toURL();
    HttpResponse downloadReportResponse =
      restClient.execute(HttpRequest.get(downloadReportURL).build(), getClientConfig().getAccessToken());
    ReportContent reportContent = GSON.fromJson(downloadReportResponse.getResponseBodyAsString(), REPORT_CONTENT_TYPE);
    Assert.assertEquals(1, reportContent.getTotal());
    Assert.assertEquals(1, reportContent.getDetails().size());
    // Assert that all the records in the report contain startMethod MANUAL
    boolean startMethodIsCorrect =
      reportContent.getDetails().stream().allMatch(content -> content.contains("\"startMethod\":\"MANUAL\""));
    if (!startMethodIsCorrect) {
      Assert.fail("All report records are expected to contain startMethod TRIGGERED, " +
                    "but actual results do not meet this requirement: " + reportContent.getDetails());
    }
  }

  private ReportGenerationInfo getReportGenerationInfo(
    RESTClient restClient, URL reportIdURL) throws IOException, UnauthenticatedException {
    HttpResponse infoResponse =
      restClient.execute(HttpRequest.get(reportIdURL).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, infoResponse.getResponseCode());
    return GSON.fromJson(infoResponse.getResponseBodyAsString(), REPORT_GEN_INFO_TYPE);
  }

}
