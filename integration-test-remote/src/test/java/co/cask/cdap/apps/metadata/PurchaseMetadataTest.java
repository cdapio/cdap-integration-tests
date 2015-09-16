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

package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.metadata.serialize.LineageRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the lineage functionality using PurchaseApp
 */
public class PurchaseMetadataTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  private static final Id.Application PURCHASE_APP = Id.Application.from(TEST_NAMESPACE, PurchaseApp.APP_NAME);
  private static final Id.Flow PURCHASE_FLOW = Id.Flow.from(PURCHASE_APP, "PurchaseFlow");
  private static final Id.Service PURCHASE_HISTORY_SERVICE = Id.Service.from(PURCHASE_APP, "PurchaseHistoryService");
  private static final Id.Workflow PURCHASE_HISTORY_WORKFLOW = Id.Workflow.from(PURCHASE_APP,
                                                                                "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER = Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE,
                                                                             "PurchaseHistoryBuilder");
  private enum ProgramAction {
    START,
    STOP
  }

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    String streamName = "purchaseStream";

    long startTime = System.currentTimeMillis();
    // assert no meta data
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                       String.format("streams/%s/lineage?start=%s&end=%s",
                                                                     streamName, startTime, Long.MAX_VALUE));

    testLineage(url, new LineageRecord(startTime, Long.MAX_VALUE, ImmutableSet.<Relation>of()));

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getId()).start();
    purchaseFlow.waitForStatus(true, 60, 1);

    StreamManager purchaseStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, 1, TimeUnit.MINUTES);

    Id.DatasetInstance dataset = Id.DatasetInstance.from(TEST_NAMESPACE, "purchases");
    Id.Stream stream = Id.Stream.from(TEST_NAMESPACE, streamName);

    List<RunRecord> ranRecords =
      programClient.getProgramRuns(PURCHASE_FLOW,
                                   ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);

    Assert.assertEquals(1, ranRecords.size());

    // check stream meta data

    LineageRecord expected =
      new LineageRecord(startTime, Long.MAX_VALUE,
                        ImmutableSet.of(
                          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),

                          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader")))
                        ));


    testLineage(url, expected);

    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    MapReduceManager purchaseHistoryBuilderManager =
      applicationManager.getMapReduceManager(PURCHASE_HISTORY_BUILDER.getId());

    // need to stop the services and flow, so we have enough resources for running workflow
    startStopServices(ProgramAction.STOP, purchaseFlow);

    purchaseHistoryWorkflowManager.start();
    purchaseHistoryWorkflowManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(false, 10 * 60, 1);
    purchaseHistoryWorkflowManager.waitForStatus(false, 60, 1);

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getId());

    makePurchaseHistoryServiceCall(purchaseHistoryService);

    Id.DatasetInstance historyDs = Id.DatasetInstance.from(TEST_NAMESPACE, "history");
    List<RunRecord> mrRanRecords =
      programClient.getProgramRuns(PURCHASE_HISTORY_BUILDER,
                                   ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(1, mrRanRecords.size());

    List<RunRecord> serviceRuns =
      programClient.getProgramRuns(PURCHASE_HISTORY_SERVICE,
                                   ProgramRunStatus.KILLED.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(1, serviceRuns.size());

    expected =
      new LineageRecord(startTime, Long.MAX_VALUE,
                        ImmutableSet.of(
                          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
                          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
                          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(mrRanRecords.get(0).getPid()))),
                          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(mrRanRecords.get(0).getPid()))),
                          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(serviceRuns.get(0).getPid())))
                        ));

    testLineage(url, expected);

    makePurchaseHistoryServiceCall(purchaseHistoryService);
    serviceRuns = programClient.getProgramRuns(PURCHASE_HISTORY_SERVICE,
                                               ProgramRunStatus.KILLED.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(2, serviceRuns.size());

    expected =
      new LineageRecord(startTime, Long.MAX_VALUE,
                        ImmutableSet.of(
                          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
                          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(ranRecords.get(0).getPid())),
                                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
                          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(mrRanRecords.get(0).getPid()))),
                          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(mrRanRecords.get(0).getPid()))),
                          // TODO : After CDAP-3623, the following will become one entry with runids in the set.
                          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(serviceRuns.get(0).getPid()))),
                          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                                       ImmutableSet.of(RunIds.fromString(serviceRuns.get(1).getPid())))
                        ));

    testLineage(url, expected);

    // check dataset lineage
    URL datasetURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                              String.format("datasets/%s/lineage?start=%s&end=%s",
                                                                            "history", startTime, Long.MAX_VALUE));
    testLineage(datasetURL, expected);
  }

  private void testLineage(URL url, LineageRecord expected) throws IOException, UnauthorizedException {
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    LineageRecord lineageRecord = GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
    Assert.assertEquals(expected, lineageRecord);
  }

  private void makePurchaseHistoryServiceCall(ServiceManager purchaseHistoryService) throws Exception {
    purchaseHistoryService.start();
    purchaseHistoryService.waitForStatus(true, 60, 1);

    URL historyURL = new URL(purchaseHistoryService.getServiceURL(), "history/Milo");

    // we have to make the first handler call after service starts with a retry
    HttpResponse response = retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(historyURL).build(),
                              getRestClient(), 120, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    // check service call to retrieve from a dataset is successful
    Assert.assertEquals(200, response.getResponseCode());

    purchaseHistoryService.stop();
    purchaseHistoryService.waitForStatus(false, 60, 1);
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
      program.waitForStatus(waitCondition, 60, 1);
    }
  }
}
