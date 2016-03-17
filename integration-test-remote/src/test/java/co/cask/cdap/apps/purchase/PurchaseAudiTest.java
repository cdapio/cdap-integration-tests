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
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistory;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProgramManager;
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
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality demonstrated in PurchaseApp
 */
public class PurchaseAudiTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  private static final Id.Application PURCHASE_APP = Id.Application.from(TEST_NAMESPACE, PurchaseApp.APP_NAME);
  private static final Id.Flow PURCHASE_FLOW = Id.Flow.from(PURCHASE_APP, "PurchaseFlow");
  private static final Id.Service PURCHASE_HISTORY_SERVICE = Id.Service.from(PURCHASE_APP, "PurchaseHistoryService");
  private static final Id.Service PURCHASE_USER_PROFILE_SERVICE = Id.Service.from(PURCHASE_APP, "UserProfileService");
  private static final Id.Workflow PURCHASE_HISTORY_WORKFLOW = Id.Workflow.from(PURCHASE_APP,
                                                                                "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER = Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE,
                                                                             "PurchaseHistoryBuilder");
  private static final Id.Schedule SCHEDULE = Id.Schedule.from(PURCHASE_APP, "DailySchedule");
  private enum ProgramAction {
    START,
    STOP
  }

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);

    // none of the programs should have any run records
    assertRuns(0, programClient, ProgramRunStatus.ALL, PURCHASE_FLOW, PURCHASE_HISTORY_SERVICE,
               PURCHASE_USER_PROFILE_SERVICE, PURCHASE_HISTORY_WORKFLOW);

    // PurchaseHistoryWorkflow should have two schedules in suspended state
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), restClient);
    List<ScheduleSpecification> workflowSchedules = scheduleClient.list(PURCHASE_HISTORY_WORKFLOW);
    Assert.assertEquals(2, workflowSchedules.size());
    checkScheduleState(scheduleClient, Scheduler.ScheduleState.SUSPENDED, workflowSchedules);

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getId()).start();
    purchaseFlow.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    StreamManager purchaseStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getId()).start();
    ServiceManager userProfileService =
      applicationManager.getServiceManager(PURCHASE_USER_PROFILE_SERVICE.getId()).start();

    userProfileService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    purchaseHistoryService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    URL serviceURL = userProfileService.getServiceURL();
    URL url = new URL(serviceURL, "user");
    String body = "{\"id\":\"Milo\",\"firstName\":\"Milo\",\"lastName\":\"Bernard\",\"categories\":[\"drink\"]}";

    // we have to make the first handler call after service starts with a retry
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.post(url).withBody(body).build());

    url = new URL(serviceURL, "user/Milo");
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(new JsonParser().parse(body), new JsonParser().parse(response.getResponseBodyAsString()));

    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    MapReduceManager purchaseHistoryBuilderManager =
      applicationManager.getMapReduceManager(PURCHASE_HISTORY_BUILDER.getId());

    // need to stop the services and flow, so we have enough resources for running workflow
    startStopServices(ProgramAction.STOP, purchaseFlow, purchaseHistoryService, userProfileService);

    purchaseHistoryWorkflowManager.start();
    purchaseHistoryWorkflowManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    purchaseHistoryBuilderManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    // wait 10 minutes for the mapreduce to execute
    purchaseHistoryBuilderManager.waitForStatus(false, 10 * 60, 1);
    purchaseHistoryWorkflowManager.waitForStatus(false, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    // Ensure that the flow and services are still running
    startStopServices(ProgramAction.START, purchaseFlow, purchaseHistoryService, userProfileService);
    Assert.assertTrue(purchaseFlow.isRunning());
    Assert.assertTrue(purchaseHistoryService.isRunning());
    Assert.assertTrue(userProfileService.isRunning());

    serviceURL = purchaseHistoryService.getServiceURL();
    url = new URL(serviceURL, "history/Milo");
    // we have to make the first handler call after service starts with a retry
    response = retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());
    PurchaseHistory purchaseHistory = GSON.fromJson(response.getResponseBodyAsString(), PurchaseHistory.class);
    Assert.assertEquals("Milo", purchaseHistory.getCustomer());

    startStopServices(ProgramAction.STOP, purchaseFlow, purchaseHistoryService, userProfileService);

    // flow and services have 'KILLED' state because they were explicitly stopped
    assertRuns(2, programClient, ProgramRunStatus.KILLED, PURCHASE_FLOW, PURCHASE_HISTORY_SERVICE,
               PURCHASE_USER_PROFILE_SERVICE);

    // workflow and mapreduce have 'COMPLETED' state because they complete on their own
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);

    // TODO: CDAP-3616 have a nextRuntime method in ScheduleClient?
    // workflow should not have a next runtime since its schedule was not 'resumed' and it was simply run once
    List<ScheduledRuntime> scheduledRuntimes = getNextRuntime(restClient);
    Assert.assertTrue(scheduledRuntimes.isEmpty());

    Assert.assertEquals("SUSPENDED", purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getId()).status(200));
    purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getId()).resume();

    scheduledRuntimes = getNextRuntime(restClient);
    Assert.assertEquals(1, scheduledRuntimes.size());

    purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getId()).suspend();
  }

  private List<ScheduledRuntime> getNextRuntime(RESTClient restClient) throws UnauthenticatedException, IOException {
    String path = String.format("apps/%s/workflows/%s/nextruntime",
                                PurchaseApp.APP_NAME, PURCHASE_HISTORY_WORKFLOW.getId());
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());

    Type scheduledRuntimeListType = new TypeToken<List<ScheduledRuntime>>() { }.getType();
    return ObjectResponse.<List<ScheduledRuntime>>fromJsonBody(response, scheduledRuntimeListType, GSON)
      .getResponseObject();
  }

  private void checkScheduleState(ScheduleClient scheduleClient, Scheduler.ScheduleState state,
                                  List<ScheduleSpecification> schedules) throws Exception {
    for (ScheduleSpecification schedule : schedules) {
      Assert.assertEquals(state.name(),
                          scheduleClient.getStatus(Id.Schedule.from(PURCHASE_APP, schedule.getSchedule().getName())));
    }
  }

  private void startStopServices(ProgramAction action, ProgramManager... programs) throws InterruptedException {
    boolean waitCondition = action == ProgramAction.START;
    for (ProgramManager program : programs) {
      if (action.equals(ProgramAction.START)) {
        program.start();
      } else {
        program.stop();
      }
    }

    for (ProgramManager program : programs) {
      program.waitForStatus(waitCondition, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    }
  }
}
