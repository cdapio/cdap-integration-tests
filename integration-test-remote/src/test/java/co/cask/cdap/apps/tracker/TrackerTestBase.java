/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.apps.tracker;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.TrackerService;
import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.TrackerMeterResult;
import co.cask.tracker.entity.ValidateTagsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TrackerTestBase
 */
public class TrackerTestBase extends AudiTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  RESTClient restClient = getRestClient();
  private static Type datasetList = new TypeToken<List<TopDatasetsResult>>() { }.getType();
  private static Type programList = new TypeToken<List<TopProgramsResult>>() { }.getType();
  private static Type applicationList = new TypeToken<List<TopApplicationsResult>>() { }.getType();
  private static Type timesinceMap = new TypeToken<Map<String, Long>>() { }.getRawType();
  private static ServiceManager trackerService;
  private static ServiceManager logServiceManager;
  private static FlowManager trackerFlow;
  private static StreamManager trackerStream;

  private URL serviceURL;
  private  URL logserviceURL;

  protected void enableTracker() throws InterruptedException, IOException {
    ApplicationManager applicationManager = deployApplication(TestTrackerApp.class);
    trackerService = applicationManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    logServiceManager = applicationManager.getServiceManager(logService.SERVICE_NAME).start();

    trackerService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerFlow = applicationManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    trackerFlow.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "testStream"));
    serviceURL = trackerService.getServiceURL();
    logserviceURL = logServiceManager.getServiceURL();
  }

  protected void waitforProcessed(long count) throws TimeoutException, InterruptedException {
    RuntimeMetrics metrics = trackerFlow.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(count, 60L, TimeUnit.SECONDS);
  }

  protected void promoteTags(String tags) throws Exception {
    URL urlPromote = new URL(serviceURL, "v1/tags/promote");
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.post(urlPromote).withBody(tags).build());
  }

  protected TagsResult getPreferredTags() throws Exception {
    URL urlGetTags = new URL(serviceURL, "v1/tags?type=preferred");
    HttpResponse response = restClient.execute(HttpRequest.get(urlGetTags).build(),
                                               getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), TagsResult.class);
  }

  protected void sendTestAuditMessages(List<AuditMessage> messages) throws IOException {
    for (AuditMessage auditMessage : messages) {
      trackerStream.send(GSON.toJson(auditMessage));
    }
  }

  protected void demoteTags(String tagsToDemote) throws IOException, UnauthenticatedException {
    URL urlDemote = new URL(serviceURL, "v1/tags/demote");
    restClient.execute(HttpRequest.post(urlDemote).withBody(tagsToDemote).build(), getClientConfig().getAccessToken());
  }

  protected void deleteTags(String tagsToDelete) throws IOException, UnauthenticatedException {
    URL urlDelete = new URL(serviceURL, new String("v1/tags/preferred?tag=" + tagsToDelete));
    restClient.execute(HttpRequest.delete(urlDelete).build(), getClientConfig().getAccessToken());
  }

  protected ValidateTagsResult validateTags(String tagsToValidate) throws IOException, UnauthenticatedException {
    URL urlValidate = new URL(serviceURL, "v1/tags/validate");
    HttpResponse validateResponse = restClient.execute(HttpRequest.post(urlValidate).withBody(tagsToValidate).build(),
                                                       getClientConfig().getAccessToken());
    ValidateTagsResult validateTagsResult = GSON.fromJson(validateResponse.getResponseBodyAsString(),
                                                          ValidateTagsResult.class);
    return validateTagsResult;
  }

  protected List<TopDatasetsResult> getTopNDatasets() throws IOException, UnauthenticatedException {
    URL urlTopNDataset = new URL(serviceURL, "v1/auditmetrics/top-entities/datasets?limit=20");
    HttpResponse datasetResponse = restClient.execute(HttpRequest.get(urlTopNDataset).build(),
                                                      getClientConfig().getAccessToken());
    return GSON.fromJson(datasetResponse.getResponseBodyAsString(), datasetList);
  }

  protected List<TopProgramsResult> getTopNPrograms() throws IOException, UnauthenticatedException {
    URL urlTopNPrograms = new URL(serviceURL, "v1/auditmetrics/top-entities/programs?limit=20");
    HttpResponse programsResponse = restClient.execute(HttpRequest.get(urlTopNPrograms).build(),
                                                       getClientConfig().getAccessToken());
    return GSON.fromJson(programsResponse.getResponseBodyAsString(), programList);
  }

  protected List<TopApplicationsResult> getTopNApplication() throws IOException, UnauthenticatedException {
    URL urlTopNApplication = new URL(serviceURL, "v1/auditmetrics/top-entities/applications?limit=20");
    HttpResponse applicationResponse = restClient.execute(HttpRequest.get(urlTopNApplication).build(),
                                                          getClientConfig().getAccessToken());
    return GSON.fromJson(applicationResponse.getResponseBodyAsString(), applicationList);
  }

  protected Map<String, Long> getTimeSince() throws IOException, UnauthenticatedException {
    URL urlTimeSince = new URL(serviceURL, "v1/auditmetrics/time-since?entityType=dataset&entityName=ds1");
    HttpResponse timeSinceResponse = restClient.execute(HttpRequest.get(urlTimeSince).build(),
                                  getClientConfig().getAccessToken());
    return GSON.fromJson(timeSinceResponse.getResponseBodyAsString(), timesinceMap);
  }

  protected AuditHistogramResult getAuditLogHistogram() throws IOException, UnauthenticatedException {
    URL urlAuditLogHistogram = new URL(serviceURL, "v1/auditmetrics/audit-histogram");
    HttpResponse audiLogHisResponse = restClient.execute(HttpRequest.get(urlAuditLogHistogram).build(),
                                                          getClientConfig().getAccessToken());
    return GSON.fromJson(audiLogHisResponse.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected TrackerMeterResult getTrackerMeter(String trackerMeter) throws IOException, UnauthenticatedException {
    URL urlTrackerMeter = new URL(serviceURL, "v1/tracker-meter");
    HttpResponse trackerMeterResponse = restClient.execute(HttpRequest.post(urlTrackerMeter)
                                             .withBody(trackerMeter).build(), getClientConfig().getAccessToken());
    return GSON.fromJson(trackerMeterResponse.getResponseBodyAsString(), TrackerMeterResult.class);
  }

  protected void startLog() throws IOException, UnauthenticatedException {
      URL urlLog = new URL(logserviceURL, "v1/loghost");
      restClient.execute(HttpRequest.get(urlLog).build(), getClientConfig().getAccessToken());
  }


}
