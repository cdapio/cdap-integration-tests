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
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests the lineage functionality using PurchaseApp
 */
public class PurchaseLineageTest extends AudiTestBase {
  private static final Gson GSON = new GsonBuilder().
    registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();

  private static final Id.Application PURCHASE_APP = Id.Application.from(TEST_NAMESPACE, PurchaseApp.APP_NAME);
  private static final Id.Flow PURCHASE_FLOW = Id.Flow.from(PURCHASE_APP, "PurchaseFlow");
  private static final Id.Service PURCHASE_HISTORY_SERVICE = Id.Service.from(PURCHASE_APP, "PurchaseHistoryService");
  private static final Id.Workflow PURCHASE_HISTORY_WORKFLOW = Id.Workflow.from(PURCHASE_APP,
                                                                                "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER = Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE,
                                                                             "PurchaseHistoryBuilder");
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();

  @Test
  public void test() throws Exception {
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    String streamName = "purchaseStream";

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long endTime = startTime + 10000;
    // assert no lineage for purchase stream.
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                       String.format("streams/%s/lineage?start=%s&end=%s",
                                                                     streamName, startTime, endTime));

    testLineage(url, LineageSerializer.toLineageRecord(startTime, endTime, new Lineage(ImmutableSet.<Relation>of())));

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getId()).start();
    purchaseFlow.waitForStatus(true, 60, 1);

    StreamManager purchaseStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, 5, TimeUnit.MINUTES);

    Id.DatasetInstance dataset = Id.DatasetInstance.from(TEST_NAMESPACE, "purchases");
    Id.Stream stream = Id.Stream.from(TEST_NAMESPACE, streamName);

    List<RunRecord> ranRecords = getRunRecords(1, programClient, PURCHASE_FLOW,
                                               ProgramRunStatus.RUNNING.name(), 0, endTime);

    // check stream lineage
    LineageRecord expected =
      LineageSerializer.toLineageRecord(
        startTime, endTime,
        new Lineage(ImmutableSet.of(
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),

          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader")))
        )));
    testLineage(url, expected);
    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    MapReduceManager purchaseHistoryBuilderManager =
      applicationManager.getMapReduceManager(PURCHASE_HISTORY_BUILDER.getId());

    purchaseFlow.stop();
    purchaseFlow.waitForStatus(false, 60, 1);

    purchaseHistoryWorkflowManager.start();
    purchaseHistoryWorkflowManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(false, 10 * 60, 1);
    purchaseHistoryWorkflowManager.waitForStatus(false, 60, 1);

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getId());

    // add tag for the dataset
    addTagToHistoryDataset("[\"dsTag1\"]");

    // add tag for the service
    addTagToPurchaseHistoryService("[\"serviceTag1\"]");

    // add metadata properties
    Map<String, String> serviceProperties = ImmutableMap.of("spKey1", "spValue1");
    addMetadataPropertiesToPurchaseHistoryService(serviceProperties);
    Map<String, String> appProperties = ImmutableMap.of("spKey1", "spApp1");
    addMetadataPropertiesToApp(appProperties);

    String firstServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    Id.DatasetInstance historyDs = Id.DatasetInstance.from(TEST_NAMESPACE, "history");
    List<RunRecord> mrRanRecords = getRunRecords(1, programClient, PURCHASE_HISTORY_BUILDER,
                                                 ProgramRunStatus.COMPLETED.name(), 0, endTime);

    List<RunRecord> serviceRuns = getRunRecords(1, programClient, PURCHASE_HISTORY_SERVICE,
                                                ProgramRunStatus.KILLED.name(), 0, endTime);

    // lineage will have mapreduce and service relations now.
    expected =
      // When CDAP-3657 is fixed, we will no longer need to use LineageSerializer for serializing.
      // Instead we can direclty use Id.toString() to get the program and data keys.
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(0).getPid()))
        )));

    testLineage(url, expected);

    // add more tags
    addTagToHistoryDataset("[\"dsTag2\"]");
    addTagToPurchaseHistoryService("[\"serviceTag2\"]");

    // add more metadata props
    serviceProperties = ImmutableMap.of("spKey2", "spValue2");
    addMetadataPropertiesToPurchaseHistoryService(serviceProperties);

    String secondServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    serviceRuns = getRunRecords(2, programClient, PURCHASE_HISTORY_SERVICE,
                                ProgramRunStatus.KILLED.name(), 0, endTime);

    expected =
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          // TODO : After CDAP-3623, the following will become one entry with runids in the set.
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(0).getPid())),
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(1).getPid()))
        )));

    testLineage(url, expected);

    // verify tags and metadata properties for the 2 service runs
    Set<MetadataRecord> expectedTagsFirst =
      ImmutableSet.of(
        new MetadataRecord(PURCHASE_APP, MetadataScope.USER, ImmutableMap.of("spKey1", "spApp1"),
                           ImmutableSet.<String>of()),
        new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER, ImmutableMap.of("spKey1", "spValue1"),
                           ImmutableSet.of("serviceTag1")),
        new MetadataRecord(historyDs, MetadataScope.USER, ImmutableMap.<String, String>of(),
                           ImmutableSet.of("dsTag1"))
      );

    verifyServiceRun(firstServiceRunId, expectedTagsFirst);

    Set<MetadataRecord> expectedTagsSecond = ImmutableSet.of(
      new MetadataRecord(PURCHASE_APP, MetadataScope.USER, ImmutableMap.of("spKey1", "spApp1"),
                         ImmutableSet.<String>of()),
      new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER,
                         ImmutableMap.of("spKey1", "spValue1", "spKey2", "spValue2"),
                         ImmutableSet.of("serviceTag1", "serviceTag2")),
      new MetadataRecord(historyDs, MetadataScope.USER,
                         ImmutableMap.<String, String>of(),
                         ImmutableSet.of("dsTag1", "dsTag2"))
    );

    verifyServiceRun(secondServiceRunId, expectedTagsSecond);

    // check dataset lineage
    URL datasetURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                              String.format("datasets/%s/lineage?start=%s&end=%s",
                                                                            "history", startTime, endTime));
    testLineage(datasetURL, expected);

    // verify search tags
    URL searchURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                             String.format("metadata/search?query=%s&target=%s",
                                                                           "service*", "PROGRAM"));
    Set<MetadataSearchResultRecord> expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      );

    verifySearchResult(searchURL, expectedSearchResults);

    // search metadata properties
    searchURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                         String.format("metadata/search?query=%s&target=%s",
                                                                       "spKey1:spValue1", "PROGRAM"));
    verifySearchResult(searchURL, expectedSearchResults);

    searchURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                         String.format("metadata/search?query=%s&target=%s",
                                                                       "spKey1:sp*", "ALL"));
    expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(PURCHASE_APP)
      );

    verifySearchResult(searchURL, expectedSearchResults);

  }

  private void verifyServiceRun(String runId, Set<MetadataRecord> expected) throws Exception {
    URL serviceRunURL = getClientConfig().resolveNamespacedURLV3(
      TEST_NAMESPACE, String.format("apps/%s/services/%s/runs/%s/metadata",
                                    PURCHASE_APP.getId(), PURCHASE_HISTORY_SERVICE.getId(), runId));

    HttpResponse response = getRestClient().execute(HttpRequest.get(serviceRunURL).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    Set<MetadataRecord> metadataRecords = GSON.fromJson(response.getResponseBodyAsString(),
                                                              SET_METADATA_RECORD_TYPE);

    Assert.assertEquals(expected, metadataRecords);
  }

  private void addTagToHistoryDataset(String body) throws Exception {
    // add tag for the dataset
    URL datasetTagURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "datasets/history/metadata/tags");
    HttpResponse response = getRestClient().execute(HttpRequest.post(datasetTagURL).withBody(body).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void addTagToPurchaseHistoryService(String body) throws Exception {
    // add tag for the service
    URL serviceTagURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                                 String.format("apps/%s/services/%s/metadata/tags",
                                                                               PURCHASE_APP.getId(),
                                                                               PURCHASE_HISTORY_SERVICE.getId()));

    HttpResponse response = getRestClient().execute(HttpRequest.post(serviceTagURL).withBody(body).
      build(), getClientConfig().getAccessToken());

    Assert.assertEquals(200, response.getResponseCode());
  }

  private void addMetadataPropertiesToApp(Map<String, String> properties) throws Exception {
    URL appPropertiesURL =
      getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, String.format("apps/%s/metadata/properties",
                                                                             PURCHASE_APP.getId()));

    HttpResponse response =
      getRestClient().execute(HttpRequest.post(appPropertiesURL).withBody(GSON.toJson(properties)).build(),
                              getClientConfig().getAccessToken());

    Assert.assertEquals(200, response.getResponseCode());
  }

  private void addMetadataPropertiesToPurchaseHistoryService(Map<String, String> properties) throws Exception {
    URL servicePropertiesURL =
      getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, String.format("apps/%s/services/%s/metadata/properties",
                                                                             PURCHASE_APP.getId(),
                                                                             PURCHASE_HISTORY_SERVICE.getId()));

    HttpResponse response =
      getRestClient().execute(HttpRequest.post(servicePropertiesURL).withBody(GSON.toJson(properties)).build(),
                              getClientConfig().getAccessToken());

    Assert.assertEquals(200, response.getResponseCode());
  }

  private void verifySearchResult(URL searchURL, Set<MetadataSearchResultRecord> expectedResults)
    throws IOException, UnauthorizedException {
    HttpResponse response = getRestClient().execute(HttpRequest.get(searchURL).build(),
                                  getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Set<MetadataSearchResultRecord> searchResults = GSON.fromJson(response.getResponseBodyAsString(),
                                                                  SET_METADATA_SEARCH_RESULT_TYPE);
    Assert.assertEquals(expectedResults, searchResults);
  }

  private void testLineage(URL url, LineageRecord expected) throws IOException, UnauthorizedException {
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    LineageRecord lineageRecord = GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
    Assert.assertEquals(expected, lineageRecord);
  }


  // starts service, makes a handler call, stops it and finally returns the runId of the completed run
  private String makePurchaseHistoryServiceCallAndReturnRunId(ServiceManager purchaseHistoryService) throws Exception {
    purchaseHistoryService.start();
    purchaseHistoryService.waitForStatus(true, 60, 1);

    URL historyURL = new URL(purchaseHistoryService.getServiceURL(), "history/Milo");

    // we have to make the first handler call after service starts with a retry
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(historyURL).build(),
                   120, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    List<RunRecord> runRecords = getRunRecords(1, getProgramClient(), PURCHASE_HISTORY_SERVICE,
                                               ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE);

    Assert.assertEquals(1, runRecords.size());
    purchaseHistoryService.stop();
    purchaseHistoryService.waitForStatus(false, 60, 1);
    return runRecords.get(0).getPid();
  }
}
