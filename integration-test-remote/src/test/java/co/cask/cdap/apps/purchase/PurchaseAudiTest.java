/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistory;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests the functionality demonstrated in PurchaseApp
 */
public class PurchaseAudiTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  private static final ApplicationId PURCHASE_APP = TEST_NAMESPACE.app(PurchaseApp.APP_NAME);
  private static final FlowId PURCHASE_FLOW = PURCHASE_APP.flow("PurchaseFlow");
  private static final ServiceId PURCHASE_HISTORY_SERVICE = PURCHASE_APP.service("PurchaseHistoryService");
  private static final ServiceId PURCHASE_USER_PROFILE_SERVICE = PURCHASE_APP.service("UserProfileService");
  private static final WorkflowId PURCHASE_HISTORY_WORKFLOW = PURCHASE_APP.workflow("PurchaseHistoryWorkflow");
  private static final ProgramId PURCHASE_HISTORY_BUILDER = PURCHASE_APP.mr("PurchaseHistoryBuilder");
  private static final ScheduleId SCHEDULE = PURCHASE_APP.schedule("DailySchedule");

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
    List<ScheduleSpecification> workflowSchedules = scheduleClient.listSchedules(PURCHASE_HISTORY_WORKFLOW);
    Assert.assertEquals(2, workflowSchedules.size());
    checkScheduleState(scheduleClient, Scheduler.ScheduleState.SUSPENDED, workflowSchedules);

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getProgram()).start();
    purchaseFlow.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    StreamManager purchaseStream = getTestManager().getStreamManager(TEST_NAMESPACE.stream("purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getProgram()).start();
    ServiceManager userProfileService =
      applicationManager.getServiceManager(PURCHASE_USER_PROFILE_SERVICE.getProgram()).start();

    userProfileService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    purchaseHistoryService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL serviceURL = userProfileService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL, "user");
    String body = "{\"id\":\"Milo\",\"firstName\":\"Milo\",\"lastName\":\"Bernard\",\"categories\":[\"drink\"]}";

    HttpResponse response =
      restClient.execute(HttpRequest.post(url).withBody(body).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    url = new URL(serviceURL, "user/Milo");
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(new JsonParser().parse(body), new JsonParser().parse(response.getResponseBodyAsString()));

    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getProgram());

    // need to stop the services and flow, so we have enough resources for running workflow
    stopServices(1, purchaseFlow, purchaseHistoryService, userProfileService);

    purchaseHistoryWorkflowManager.start();
    // wait 10 minutes for the mapreduce to execute
    purchaseHistoryWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    startServices(purchaseHistoryService);
    Assert.assertTrue(purchaseHistoryService.isRunning());

    serviceURL = purchaseHistoryService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    url = new URL(serviceURL, "history/Milo");
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    PurchaseHistory purchaseHistory = GSON.fromJson(response.getResponseBodyAsString(), PurchaseHistory.class);
    Assert.assertEquals("Milo", purchaseHistory.getCustomer());

    stopServices(2, purchaseHistoryService);

    // workflow and mapreduce have 'COMPLETED' state because they complete on their own
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);

    // workflow should not have a next runtime since its schedule was not 'resumed' and it was simply run once
    List<ScheduledRuntime> scheduledRuntimes = scheduleClient.nextRuntimes(PURCHASE_HISTORY_WORKFLOW);
    Assert.assertTrue(scheduledRuntimes.isEmpty());

    Assert.assertEquals("SUSPENDED", purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getSchedule()).status(200));
    purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getSchedule()).resume();

    scheduledRuntimes = scheduleClient.nextRuntimes(PURCHASE_HISTORY_WORKFLOW);
    Assert.assertEquals(1, scheduledRuntimes.size());

    purchaseHistoryWorkflowManager.getSchedule(SCHEDULE.getSchedule()).suspend();
  }

  private void checkScheduleState(ScheduleClient scheduleClient, Scheduler.ScheduleState state,
                                  List<ScheduleSpecification> schedules) throws Exception {
    for (ScheduleSpecification schedule : schedules) {
      Assert.assertEquals(state.name(),
                          scheduleClient.getStatus(PURCHASE_APP.schedule(schedule.getSchedule().getName())));
    }
  }

  private void startServices(ProgramManager... programs)
    throws InterruptedException, TimeoutException, ExecutionException {
    for (ProgramManager program : programs) {
        program.start();
    }
    for (ProgramManager program : programs) {
      program.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private void stopServices(int runCount, ProgramManager... programs)
    throws InterruptedException, TimeoutException, ExecutionException {
    for (ProgramManager program : programs) {
      program.stop();
    }
    for (ProgramManager program : programs) {
      program.waitForRuns(ProgramRunStatus.KILLED, runCount, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }
}
