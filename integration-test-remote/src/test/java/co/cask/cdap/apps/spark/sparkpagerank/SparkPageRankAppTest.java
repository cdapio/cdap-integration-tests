/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps.spark.sparkpagerank;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.sparkpagerank.SparkPageRankApp;
import co.cask.cdap.examples.sparkpagerank.SparkPageRankProgram;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of {@link SparkPageRankApp}
 */
public class SparkPageRankAppTest extends AudiTestBase {

  private static final String URL_1 = "http://example.com/page1";
  private static final String URL_2 = "http://example.com/page2";
  private static final String URL_3 = "http://example.com/page3";
  private static final String RANK = "14";
  private static final String TOTAL_PAGES = "1";

  private static final Id.Application SPARK_PAGE_RANK_APP = Id.Application.from(TEST_NAMESPACE, "SparkPageRank");
  private static final Id.Stream STREAM = Id.Stream.from(TEST_NAMESPACE, SparkPageRankApp.BACKLINK_URL_STREAM);
  private static final Id.Service RANKS_SERVICE = Id.Service.from(
    SPARK_PAGE_RANK_APP, SparkPageRankApp.RANKS_SERVICE_NAME);
  private static final Id.Service GOOGLE_TYPE_PR_SERVICE = Id.Service.from(
    SPARK_PAGE_RANK_APP, SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
  private static final Id.Service TOTAL_PAGES_PR_SERVICE = Id.Service.from(
    SPARK_PAGE_RANK_APP, SparkPageRankApp.TOTAL_PAGES_PR_SERVICE_NAME);
  private static final Id.Workflow PAGE_RANK_WORKFLOW = Id.Workflow.from(
    SPARK_PAGE_RANK_APP, SparkPageRankApp.PageRankWorkflow.class.getSimpleName());
  private static final Id.Program RANKS_COUNTER_PROGRAM = Id.Program.from(
    SPARK_PAGE_RANK_APP, ProgramType.MAPREDUCE, SparkPageRankApp.RanksCounter.class.getSimpleName());
  private static final Id.Program PAGE_RANK_PROGRAM = Id.Program.from(SPARK_PAGE_RANK_APP, ProgramType.SPARK,
                                                                      SparkPageRankProgram.class.getSimpleName());


  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    final ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(SparkPageRankApp.class);

    // none of the programs should have any run records
    Assert.assertEquals(0, programClient.getAllProgramRuns(RANKS_SERVICE, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(GOOGLE_TYPE_PR_SERVICE,
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(TOTAL_PAGES_PR_SERVICE,
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(PAGE_RANK_WORKFLOW,
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(RANKS_COUNTER_PROGRAM,
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(PAGE_RANK_PROGRAM,
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());

    // PageRankWorkflow should have no schedules
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), restClient);
    List<ScheduleSpecification> workflowSchedules = scheduleClient.list(PAGE_RANK_WORKFLOW);
    Assert.assertEquals(0, workflowSchedules.size());

    StreamManager backlinkURLStream = getTestManager().getStreamManager(STREAM);
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_2));
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_3));
    backlinkURLStream.send(Joiner.on(" ").join(URL_2, URL_1));
    backlinkURLStream.send(Joiner.on(" ").join(URL_3, URL_1));

    // Start GoogleTypePR
    ServiceManager transformServiceManager = applicationManager.getServiceManager(GOOGLE_TYPE_PR_SERVICE
                                                                                    .getId()).start();
    // Start RanksService
    ServiceManager ranksServiceManager = applicationManager.getServiceManager(RANKS_SERVICE
                                                                                .getId()).start();
    // Start TotalPagesPRService
    ServiceManager totalPagesServiceManager = applicationManager.getServiceManager(TOTAL_PAGES_PR_SERVICE
                                                                                     .getId()).start();
    // Wait for services to start
    transformServiceManager.waitForStatus(true, 60, 1);
    ranksServiceManager.waitForStatus(true, 60, 1);
    totalPagesServiceManager.waitForStatus(true, 60, 1);

    // TODO: better way to wait for service to be up.
    TimeUnit.SECONDS.sleep(60);

    WorkflowManager pageRankWorkflowManager = applicationManager.getWorkflowManager(PAGE_RANK_WORKFLOW.getId());

    MapReduceManager ranksCounterManager = applicationManager.getMapReduceManager(RANKS_COUNTER_PROGRAM.getId());
    SparkManager pageRankManager = applicationManager.getSparkManager(PAGE_RANK_PROGRAM.getId());

    pageRankWorkflowManager.start();

    pageRankWorkflowManager.waitForStatus(true, 60, 1);
    // wait until the spark program is running or completes. It completes too fast on standalone to rely on
    // programManager#waitForStatus(true, ...)
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        List<RunRecord> pageRankRuns =
          programClient.getAllProgramRuns(PAGE_RANK_PROGRAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
        if (pageRankRuns.size() != 1) {
          return false;
        }
        ProgramRunStatus status = pageRankRuns.get(0).getStatus();
        return (status == ProgramRunStatus.RUNNING || status == ProgramRunStatus.COMPLETED);
      }
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    pageRankManager.waitForStatus(false, 10 * 60, 1);
    ranksCounterManager.waitForStatus(true, 60, 1);
    ranksCounterManager.waitForStatus(false, 10 * 60, 1);
    pageRankWorkflowManager.waitForStatus(false, 60, 1);

    // Ensure that the services are still running
    Assert.assertTrue(transformServiceManager.isRunning());
    Assert.assertTrue(ranksServiceManager.isRunning());
    Assert.assertTrue(totalPagesServiceManager.isRunning());

    URL url = new URL(totalPagesServiceManager.getServiceURL(),
                      SparkPageRankApp.TotalPagesHandler.TOTAL_PAGES_PATH + "/" + RANK);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(TOTAL_PAGES, response.getResponseBodyAsString());

    url = new URL(ranksServiceManager.getServiceURL(), SparkPageRankApp.RanksServiceHandler.RANKS_SERVICE_PATH);
    response = restClient.execute(HttpRequest
                                    .post(url)
                                    .withBody("{\"url\":\"" + URL_1 + "\"}")
                                    .build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(RANK, response.getResponseBodyAsString());

    // stop all services
    ranksServiceManager.stop();
    totalPagesServiceManager.stop();
    transformServiceManager.stop();

    // Wait for services to stop
    transformServiceManager.waitForStatus(false, 60, 1);
    ranksServiceManager.waitForStatus(false, 60, 1);
    totalPagesServiceManager.waitForStatus(false, 60, 1);

    // services should 'KILLED' state because they were explicitly stopped
    List<RunRecord> serviceRuns = programClient.getAllProgramRuns(RANKS_SERVICE, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(serviceRuns, ProgramRunStatus.KILLED);

    serviceRuns = programClient.getAllProgramRuns(GOOGLE_TYPE_PR_SERVICE, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(serviceRuns, ProgramRunStatus.KILLED);

    serviceRuns = programClient.getAllProgramRuns(TOTAL_PAGES_PR_SERVICE, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(serviceRuns, ProgramRunStatus.KILLED);

    // workflow, mapreduce and spark should have 'COMPLETED' state because they complete on their own
    List<RunRecord> programRuns = programClient.getAllProgramRuns(PAGE_RANK_WORKFLOW, 0, Long.MAX_VALUE,
                                                                  Integer.MAX_VALUE);
    assertSingleRun(programRuns, ProgramRunStatus.COMPLETED);

    programRuns = programClient.getAllProgramRuns(RANKS_COUNTER_PROGRAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(programRuns, ProgramRunStatus.COMPLETED);

    programRuns = programClient.getAllProgramRuns(PAGE_RANK_PROGRAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(programRuns, ProgramRunStatus.COMPLETED);

  }

  private void assertSingleRun(List<RunRecord> runRecords, ProgramRunStatus expectedStatus) {
    Assert.assertEquals(1, runRecords.size());
    Assert.assertEquals(expectedStatus, runRecords.get(0).getStatus());
  }
}
