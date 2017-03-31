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

package co.cask.cdap.upgrade;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Upgrade tests for App meta data such as run records, workflow node state, and workflow token
 */
public class AppMetaDataUpgradeTest extends UpgradeTestBase {
  private static final Gson GSON = new Gson();
  private static final Id.Application PURCHASE_APP = Id.Application.from(TEST_NAMESPACE, PurchaseApp.APP_NAME);
  private static final Id.Stream PURCHASE_STREAM = Id.Stream.from(TEST_NAMESPACE, "purchaseStream");
  private static final Id.Flow PURCHASE_FLOW = Id.Flow.from(PURCHASE_APP, "PurchaseFlow");
  private static final Id.Service PURCHASE_HISTORY_SERVICE = Id.Service.from(PURCHASE_APP, "PurchaseHistoryService");
  private static final Id.Service PURCHASE_USER_PROFILE_SERVICE = Id.Service.from(PURCHASE_APP, "UserProfileService");
  private static final Id.Workflow PURCHASE_HISTORY_WORKFLOW = Id.Workflow.from(PURCHASE_APP,
                                                                                "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER = Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE,
                                                                             "PurchaseHistoryBuilder");
  private enum ProgramAction {
    START,
    STOP
  }

  @Override
  protected void preStage() throws Exception {
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);

    // none of the programs should have any run records
    assertRuns(0, programClient, ProgramRunStatus.ALL, PURCHASE_FLOW, PURCHASE_HISTORY_SERVICE,
               PURCHASE_USER_PROFILE_SERVICE, PURCHASE_HISTORY_WORKFLOW);

    // Run programs in PurchaseApp with userId Milo and assert result: PurchaseFlow, PurchaseHistoryService,
    // PurchaseUserProfileService and PurchaseHistoryWorkflow for once
    runPurchaseAppPrograms(applicationManager, restClient, "Milo");

    // Flow and services have one 'KILLED' record because they were explicitly stopped for once
    assertRuns(1, programClient, ProgramRunStatus.KILLED, PURCHASE_FLOW, PURCHASE_HISTORY_SERVICE,
               PURCHASE_USER_PROFILE_SERVICE);
    // Workflow and mapreduce in the workflow should have one 'COMPLETED' record
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);
  }


  @Override
  protected void postStage() throws Exception {
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();
    ApplicationManager applicationManager = getApplicationManager(PURCHASE_APP.toEntityId());
    // Assert purchaseFlow, purchaseHistoryService, userProfileService have 1 killed runs
    assertRuns(1, programClient, ProgramRunStatus.KILLED, PURCHASE_FLOW, PURCHASE_HISTORY_SERVICE,
               PURCHASE_USER_PROFILE_SERVICE);

    WorkflowManager purchaseHistoryWorkflow = applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    // Assert PurchaseHistoryWorkflow and PurchaseHistoryBuilder have 1 completed run from preStage
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);
    RunRecord record = purchaseHistoryWorkflow.getHistory().get(0);

    // Assert PurchaseHistoryWorkflow user token from preStage can be retrieved but contains no data
    Assert.assertTrue(
      purchaseHistoryWorkflow.getToken(record.getPid(), WorkflowToken.Scope.USER, null).getTokenData().size() == 0);
    // Assert PurchaseHistoryWorkflow system token from preStage can be retrieved and contains data
    Assert.assertTrue(
      purchaseHistoryWorkflow.getToken(record.getPid(), WorkflowToken.Scope.SYSTEM, null).getTokenData().size() > 0);

    // Assert PurchaseHistoryWorkflow has one entry in node state detail from preStage
    Map<String, WorkflowNodeStateDetail> nodeStateDetailMap =
      purchaseHistoryWorkflow.getWorkflowNodeStates(record.getPid());
    Assert.assertEquals(1, nodeStateDetailMap.size());
    // Assert the content of PurchaseHistoryWorkflow's node state detail from preStage
    Map.Entry<String, WorkflowNodeStateDetail> entry = nodeStateDetailMap.entrySet().iterator().next();
    Assert.assertEquals(PURCHASE_HISTORY_BUILDER.getId(), entry.getKey());
    Assert.assertEquals(NodeStatus.COMPLETED, entry.getValue().getNodeStatus());

    // Query PurchaseHistoryService with userId Milo to verify query result again from preStage
    checkPurchaseHistoryResult(applicationManager, restClient, "Milo");

    // Run programs in PurchaseApp again after upgrade with another userId Alice and assert result
    runPurchaseAppPrograms(applicationManager, restClient, "Alice");

    // PurchaseFlow and UserProfileService have two 'KILLED' record, one from preStage and one from postStage
    assertRuns(2, programClient, ProgramRunStatus.KILLED, PURCHASE_FLOW, PURCHASE_USER_PROFILE_SERVICE);
    // PurchaseHistoryService has been killed three times, once at pre-stage and
    // twice at post-stage by calling checkPurchaseHistoryResult and runPurchaseAppPrograms
    assertRuns(3, programClient, ProgramRunStatus.KILLED, PURCHASE_HISTORY_SERVICE);
    // Workflow and mapreduce in the workflow should have two 'COMPLETED' record, one from preStage
    // and one from postStage
    assertRuns(2, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);
  }

  private void runPurchaseAppPrograms(ApplicationManager applicationManager, RESTClient restClient, String userId)
    throws InterruptedException, IOException, UnauthorizedException, UnauthenticatedException, TimeoutException {
    // start PurchaseFlow
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getId()).start();
    // Ingest an event into purchaseStream
    StreamManager purchaseStream = getTestManager().getStreamManager(PURCHASE_STREAM);
    purchaseStream.send(userId + " bought 10 PBR for $12");
    // Wait for the 1 event in purchaseStream to be processed by PurchaseFlow
    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Start UserProfileService and wait for it to be available
    ServiceManager userProfileService =
      applicationManager.getServiceManager(PURCHASE_USER_PROFILE_SERVICE.getId()).start();
    URL serviceURL = userProfileService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Send a user profile with userId to the UserProfileService and assert sending is successful
    URL postUrl = new URL(serviceURL, "user");
    String body = String.format(
      "{\"id\":\"%s\",\"firstName\":\"%s\",\"lastName\":\"Bernard\",\"categories\":[\"drink\"]}", userId, userId);
    HttpResponse response =
      restClient.execute(HttpRequest.post(postUrl).withBody(body).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Query user profile of userId from UserProfileService and assert the response is correct
    URL queryUrl = new URL(serviceURL, "user/" + userId);
    response = restClient.execute(HttpRequest.get(queryUrl).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(new JsonParser().parse(body), new JsonParser().parse(response.getResponseBodyAsString()));

    // Stop services and flow, so we have enough resources for running workflow
    startStopServices(ProgramAction.STOP, purchaseFlow, userProfileService);

    // Start PurchaseHistoryWorkflow
    WorkflowManager purchaseHistoryWorkflow = applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    purchaseHistoryWorkflow.start();
    // Wait 10 minutes for the PurchaseHistoryWorkflow to complete
    purchaseHistoryWorkflow.waitForStatus(false, 600, 1);
    // Verify PurchaseHistoryWorkflow result with PurchaseHistoryService
    checkPurchaseHistoryResult(applicationManager, restClient, userId);
  }

  private void checkPurchaseHistoryResult(ApplicationManager applicationManager, RESTClient restClient, String userId)
    throws InterruptedException, IOException, UnauthorizedException, UnauthenticatedException {

    // Start PurchaseHistoryService and ensure that the service is available
    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getId()).start();
    URL serviceURL = purchaseHistoryService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // Query purchase history of userId and assert the response is correct
    URL url = new URL(serviceURL, "history/" + userId);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    PurchaseHistory purchaseHistory = GSON.fromJson(response.getResponseBodyAsString(), PurchaseHistory.class);
    Assert.assertEquals(userId, purchaseHistory.getCustomer());
    // Stop PurchaseHistoryService
    startStopServices(ProgramAction.STOP, purchaseHistoryService);
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

