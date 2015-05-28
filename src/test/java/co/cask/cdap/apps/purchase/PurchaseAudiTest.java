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

package co.cask.cdap.apps.purchase;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality demonstrated in PurchaseApp
 */
public class PurchaseAudiTest extends AudiTestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);

    // none of the programs should have any run records
    Assert.assertEquals(0, programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.FLOW,
                                                           "PurchaseFlow",
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.SERVICE,
                                                           "PurchaseHistoryService",
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.SERVICE,
                                                           "UserProfileService",
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.WORKFLOW,
                                                           "PurchaseHistoryWorkflow",
                                                           0, Long.MAX_VALUE, Integer.MAX_VALUE).size());

    // PurchaseHistoryWorkflow should have two schedules
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), restClient);
    List<ScheduleSpecification> workflowSchedules =
      scheduleClient.list(PurchaseApp.APP_NAME, "PurchaseHistoryWorkflow");
    Assert.assertEquals(2, workflowSchedules.size());

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.startFlow("PurchaseFlow");
    purchaseFlow.waitForStatus(true, 60, 1);

    StreamManager purchaseStream =
      getTestManager().getStreamManager(Id.Stream.from(Constants.DEFAULT_NAMESPACE, "purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, 1, TimeUnit.MINUTES);

    ServiceManager purchaseHistoryService = applicationManager.startService("PurchaseHistoryService");
    ServiceManager userProfileService = applicationManager.startService("UserProfileService");

    userProfileService.waitForStatus(true, 60, 1);
    purchaseHistoryService.waitForStatus(true, 60, 1);

    // TODO: better way to wait for service to be up.
    TimeUnit.SECONDS.sleep(60);
    URL serviceURL = userProfileService.getServiceURL();
    URL url = new URL(serviceURL, "user");
    String body = "{\"id\":\"Milo\",\"firstName\":\"Milo\",\"lastName\":\"Bernard\",\"categories\":[\"drink\"]}";
    // TODO: retries? Because service handler may not be ready, even though program status is 'RUNNING'
    HttpResponse response =
      restClient.execute(HttpRequest.post(url).withBody(body).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    url = new URL(serviceURL, "user/Milo");
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(new JsonParser().parse(body), new JsonParser().parse(response.getResponseBodyAsString()));

    WorkflowManager purchaseHistoryWorkflowManager = applicationManager.getWorkflowManager("PurchaseHistoryWorkflow");
    MapReduceManager purchaseHistoryBuilderManager = applicationManager.getMapReduceManager("PurchaseHistoryBuilder");

    purchaseHistoryWorkflowManager.start();
    purchaseHistoryWorkflowManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(false, 10 * 60, 1);
    purchaseHistoryWorkflowManager.waitForStatus(false, 60, 1);

    // Ensure that the flow and services are still running
    Assert.assertTrue(purchaseFlow.isRunning());
    Assert.assertTrue(purchaseHistoryService.isRunning());
    Assert.assertTrue(userProfileService.isRunning());

    serviceURL = purchaseHistoryService.getServiceURL();
    url = new URL(serviceURL, "history/Milo");
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    PurchaseHistory purchaseHistory = GSON.fromJson(response.getResponseBodyAsString(), PurchaseHistory.class);
    Assert.assertEquals("Milo", purchaseHistory.getCustomer());

    // should not delete application when programs are running
    ApplicationClient appClient = new ApplicationClient(getClientConfig(), restClient);
    try {
      appClient.delete(PurchaseApp.APP_NAME);
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertEquals("403: Program is still running", expected.getMessage());
    }

    // should not delete non-existing application (TODO: unrelated. move to a different test case?)
    try {
      appClient.delete("NoSuchApp");
      Assert.fail();
    } catch (ApplicationNotFoundException expected) {
    }

    purchaseFlow.stop();
    purchaseHistoryService.stop();
    userProfileService.stop();

    purchaseFlow.waitForStatus(false, 60, 1);
    purchaseHistoryService.waitForStatus(false, 60, 1);
    userProfileService.waitForStatus(false, 60, 1);

    // flow and services have 'KILLED' state because they were explicitly stopped
    List<RunRecord> purchaseFlowRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.FLOW, "PurchaseFlow",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(purchaseFlowRuns, ProgramRunStatus.KILLED);

    List<RunRecord> purchaseHistoryServiceRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.SERVICE, "PurchaseHistoryService",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(purchaseHistoryServiceRuns, ProgramRunStatus.KILLED);

    List<RunRecord> userProfileServiceRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.SERVICE, "UserProfileService",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(userProfileServiceRuns, ProgramRunStatus.KILLED);

    // workflow and mapreduce have 'COMPLETED' state because they complete on their own
    List<RunRecord> workflowRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(workflowRuns, ProgramRunStatus.COMPLETED);

    List<RunRecord> mapReduceRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(mapReduceRuns, ProgramRunStatus.COMPLETED);

    // TODO: have a nextRuntime method in ScheduleClient?
    // workflow should have a next runtime
    String path = String.format("apps/%s/workflows/%s/nextruntime", PurchaseApp.APP_NAME, "PurchaseHistoryWorkflow");
    url = getClientConfig().resolveNamespacedURLV3(path);
    response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());

    Type scheduledRuntimeListType = new TypeToken<List<ScheduledRuntime>>() { }.getType();
    List<ScheduledRuntime> scheduledRuntimes =
      ObjectResponse.<List<ScheduledRuntime>>fromJsonBody(response, scheduledRuntimeListType, GSON)
        .getResponseObject();
    Assert.assertEquals(1, scheduledRuntimes.size());
  }

  private void assertSingleRun(List<RunRecord> runRecords, ProgramRunStatus expectedStatus) {
    Assert.assertEquals(1, runRecords.size());
    Assert.assertEquals(expectedStatus, runRecords.get(0).getStatus());
  }
}
