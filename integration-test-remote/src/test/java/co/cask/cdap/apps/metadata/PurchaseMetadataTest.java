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

package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.LineageClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the metadata and lineage functionality using PurchaseApp
 */
public class PurchaseMetadataTest extends AudiTestBase {
  private static final ApplicationId PURCHASE_APP = TEST_NAMESPACE.app(PurchaseApp.APP_NAME);
  private static final ProgramId PURCHASE_FLOW = PURCHASE_APP.flow("PurchaseFlow");
  private static final ProgramId PURCHASE_HISTORY_SERVICE = PURCHASE_APP.service("PurchaseHistoryService");
  private static final ProgramId CATALOG_LOOKUP_SERVICE = PURCHASE_APP.service("CatalogLookup");
  private static final ProgramId USER_PROFILE_SERVICE = PURCHASE_APP.service("UserProfileService");
  private static final ProgramId PURCHASE_HISTORY_WORKFLOW = PURCHASE_APP.workflow("PurchaseHistoryWorkflow");
  private static final ProgramId PURCHASE_HISTORY_BUILDER = PURCHASE_APP.mr("PurchaseHistoryBuilder");
  private static final StreamId PURCHASE_STREAM = TEST_NAMESPACE.stream("purchaseStream");
  private static final DatasetId HISTORY_DS = TEST_NAMESPACE.dataset("history");
  private static final DatasetId PURCHASES_DS = TEST_NAMESPACE.dataset("purchases");
  private static final DatasetId FREQUENT_CUSTOMERS_DS = TEST_NAMESPACE.dataset("frequentCustomers");
  private static final DatasetId USER_PROFILES_DS = TEST_NAMESPACE.dataset("userProfiles");

  private MetadataClient metadataClient;
  private LineageClient lineageClient;

  @Before
  public void setup() {
    metadataClient = new MetadataClient(getClientConfig(), getRestClient());
    lineageClient = new LineageClient(getClientConfig(), getRestClient());
  }

  @Test
  public void testLineage() throws Exception {
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long endTime = startTime + 10000;
    // assert no lineage for purchase stream.
    Assert.assertEquals(LineageSerializer.toLineageRecord(startTime, endTime, new Lineage(ImmutableSet.<Relation>of()),
                                                          ImmutableSet.<CollapseType>of()),
                        lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getEntityName()).start();
    purchaseFlow.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    StreamManager purchaseStream = getTestManager().getStreamManager(TEST_NAMESPACE.stream("purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    List<RunRecord> ranRecords = getRunRecords(1, programClient, PURCHASE_FLOW,
                                               ProgramRunStatus.RUNNING.name(), 0, endTime);
    // check stream lineage
    LineageRecord expected =
      LineageSerializer.toLineageRecord(
        startTime, endTime,
        new Lineage(ImmutableSet.of(
          new Relation(PURCHASES_DS, PURCHASE_FLOW, AccessType.WRITE,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("collector"))),
          new Relation(PURCHASE_STREAM, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("reader")))
        )), ImmutableSet.of());

    // wait for lineage to be written
    waitFor(expected, () -> lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getEntityName());

    purchaseFlow.stop();
    purchaseFlow.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    purchaseHistoryWorkflowManager.start();
    // wait 10 minutes for the mapreduce to execute
    purchaseHistoryWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // add tag for the dataset
    Set<String> historyDatasetTags = ImmutableSet.of("dsTag1");
    metadataClient.addTags(HISTORY_DS, historyDatasetTags);

    // add tag for the service
    Set<String> purchaseHistoryServiceTags = ImmutableSet.of("serviceTag1");
    metadataClient.addTags(PURCHASE_HISTORY_SERVICE, purchaseHistoryServiceTags);

    // assert that the tags we added exist
    Assert.assertEquals(historyDatasetTags, metadataClient.getTags(HISTORY_DS, MetadataScope.USER));
    Assert.assertEquals(purchaseHistoryServiceTags,
                        metadataClient.getTags(PURCHASE_HISTORY_SERVICE, MetadataScope.USER));

    // add metadata properties
    Map<String, String> serviceProperties = ImmutableMap.of("spKey1", "spValue1");
    metadataClient.addProperties(PURCHASE_HISTORY_SERVICE, serviceProperties);
    Map<String, String> appProperties = ImmutableMap.of("spKey1", "spApp1");
    metadataClient.addProperties(PURCHASE_APP, appProperties);

    // assert that the properties that we added exists
    Assert.assertEquals(serviceProperties, metadataClient.getProperties(PURCHASE_HISTORY_SERVICE,
                                                                        MetadataScope.USER));
    Assert.assertEquals(appProperties, metadataClient.getProperties(PURCHASE_APP, MetadataScope.USER));

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getEntityName());
    String firstServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    List<RunRecord> mrRanRecords = getRunRecords(1, programClient, PURCHASE_HISTORY_BUILDER,
                                                 ProgramRunStatus.COMPLETED.name(), 0, endTime);

    List<RunRecord> serviceRuns = getRunRecords(1, programClient, PURCHASE_HISTORY_SERVICE,
                                                ProgramRunStatus.KILLED.name(), 0, endTime);

    // lineage will have mapreduce and service relations now.
    expected =
      // When CDAP-3657 is fixed, we will no longer need to use LineageSerializer for serializing.
      // Instead we can directly use Id.toString() to get the program and data keys.
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(PURCHASE_STREAM, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("reader"))),
          new Relation(PURCHASES_DS, PURCHASE_FLOW, AccessType.WRITE,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("collector"))),
          new Relation(HISTORY_DS, PURCHASE_HISTORY_BUILDER, AccessType.WRITE,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(PURCHASES_DS, PURCHASE_HISTORY_BUILDER, AccessType.READ,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(FREQUENT_CUSTOMERS_DS, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(HISTORY_DS, PURCHASE_HISTORY_SERVICE, AccessType.READ,
                       RunIds.fromString(serviceRuns.get(0).getPid()))
        )), ImmutableSet.of());

    waitFor(expected, () -> lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // add more tags
    metadataClient.addTags(HISTORY_DS, ImmutableSet.of("dsTag2"));
    metadataClient.addTags(PURCHASE_HISTORY_SERVICE, ImmutableSet.of("serviceTag2"));

    // add more metadata props
    serviceProperties = ImmutableMap.of("spKey2", "spValue2");
    metadataClient.addProperties(PURCHASE_HISTORY_SERVICE, serviceProperties);

    String secondServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    serviceRuns = getRunRecords(2, programClient, PURCHASE_HISTORY_SERVICE,
                                ProgramRunStatus.KILLED.name(), 0, endTime);

    expected =
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(PURCHASE_STREAM, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("reader"))),
          new Relation(PURCHASES_DS, PURCHASE_FLOW, AccessType.WRITE,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(PURCHASE_FLOW.flowlet("collector"))),
          new Relation(HISTORY_DS, PURCHASE_HISTORY_BUILDER, AccessType.WRITE,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(PURCHASES_DS, PURCHASE_HISTORY_BUILDER, AccessType.READ,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(FREQUENT_CUSTOMERS_DS, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          // TODO : After CDAP-3623, the following will become one entry with runids in the set.
          new Relation(HISTORY_DS, PURCHASE_HISTORY_SERVICE, AccessType.READ,
                       RunIds.fromString(serviceRuns.get(0).getPid())),
          new Relation(HISTORY_DS, PURCHASE_HISTORY_SERVICE, AccessType.READ,
                       RunIds.fromString(serviceRuns.get(1).getPid()))
        )), ImmutableSet.of());

    waitFor(expected, () -> lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // verify tags and metadata properties for the 2 service runs
    Set<MetadataRecord> expectedTagsFirst =
      ImmutableSet.of(
        new MetadataRecord(PURCHASE_APP, MetadataScope.USER, appProperties,
                           ImmutableSet.of()),
        new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER, ImmutableMap.of("spKey1", "spValue1"),
                           ImmutableSet.of("serviceTag1")),
        new MetadataRecord(HISTORY_DS, MetadataScope.USER, ImmutableMap.of(),
                           ImmutableSet.of("dsTag1"))
      );

    waitFor(expectedTagsFirst,
            () -> metadataClient.getMetadata(PURCHASE_HISTORY_SERVICE.run(firstServiceRunId)));

    Set<MetadataRecord> expectedTagsSecond = ImmutableSet.of(
      new MetadataRecord(PURCHASE_APP, MetadataScope.USER, appProperties,
                         ImmutableSet.of()),
      new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER,
                         ImmutableMap.of("spKey1", "spValue1", "spKey2", "spValue2"),
                         ImmutableSet.of("serviceTag1", "serviceTag2")),
      new MetadataRecord(HISTORY_DS, MetadataScope.USER,
                         ImmutableMap.of(),
                         ImmutableSet.of("dsTag1", "dsTag2"))
    );

    Set<String> purchaseHistoryServiceTwoTags = ImmutableSet.of("serviceTag1", "serviceTag2");
    Map<String, String> purchaseHistoryServiceTwoProps = ImmutableMap.of("spKey1", "spValue1", "spKey2", "spValue2");
    // assert tags and props for second run.
    Assert.assertEquals(purchaseHistoryServiceTwoTags,
                        metadataClient.getTags(PURCHASE_HISTORY_SERVICE, MetadataScope.USER));
    Assert.assertEquals(purchaseHistoryServiceTwoProps, metadataClient.getProperties(PURCHASE_HISTORY_SERVICE,
                                                                                     MetadataScope.USER));

    waitFor(expectedTagsSecond,
            () -> metadataClient.getMetadata(PURCHASE_HISTORY_SERVICE.run(secondServiceRunId)));

    // check dataset lineage
    waitFor(expected, () -> lineageClient.getLineage(HISTORY_DS, startTime, endTime, null));

    // verify search tags
    Set<MetadataSearchResultRecord> expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      );

    Assert.assertEquals(
      expectedSearchResults,
      searchMetadata(TEST_NAMESPACE, "service*", EntityTypeSimpleName.PROGRAM)
    );

    // search metadata properties
    expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      );
    Assert.assertEquals(
      expectedSearchResults,
      searchMetadata(TEST_NAMESPACE, "spKey1:spValue1", EntityTypeSimpleName.PROGRAM)
    );

    expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(PURCHASE_APP)
      );

    Assert.assertEquals(
      expectedSearchResults,
      searchMetadata(TEST_NAMESPACE, "spKey1:sp*", EntityTypeSimpleName.ALL));
  }

  @Test
  public void testSearchUsingSystemMetadata() throws Exception {
    deployApplication(PurchaseApp.class);
    // search artifacts
    assertArtifactSearch();
    // search app
    assertAppSearch();
    // search programs
    assertProgramSearch();
    // search data entities
    assertDataEntitySearch();
  }

  private <T> void waitFor(T expected, Callable<T> callable) throws Exception {
    Tasks.waitFor(expected, callable, 60, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }

  private void assertArtifactSearch() throws Exception {
    String artifactName = "system-metadata-artifact";
    String pluginArtifactName = "system-metadata-plugins";
    ArtifactId systemMetadataArtifact = TEST_NAMESPACE.artifact(artifactName, "1.0.0");
    getTestManager().addAppArtifact(systemMetadataArtifact, ArtifactSystemMetadataApp.class);
    ArtifactId pluginArtifact = TEST_NAMESPACE.artifact(pluginArtifactName, "1.0.0");
    getTestManager().addPluginArtifact(pluginArtifact, systemMetadataArtifact,
                                       ArtifactSystemMetadataApp.EchoPlugin1.class,
                                       ArtifactSystemMetadataApp.EchoPlugin2.class);
    // verify search using artifact name
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemMetadataArtifact)),
      searchMetadata(TEST_NAMESPACE, artifactName, null)
    );
    // verify search using plugin name
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(pluginArtifact)),
      searchMetadata(TEST_NAMESPACE,
                     ArtifactSystemMetadataApp.PLUGIN1_NAME, EntityTypeSimpleName.ARTIFACT)
    );
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(pluginArtifact)),
      searchMetadata(TEST_NAMESPACE,
                     ArtifactSystemMetadataApp.PLUGIN2_NAME, null)
    );
  }

  private void assertAppSearch() throws Exception {
    // using app name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PURCHASE_APP.getEntityName(), null));
    // using program names
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PURCHASE_FLOW.getEntityName(),
                                                 EntityTypeSimpleName.APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_BUILDER.getEntityName(),
                                                 EntityTypeSimpleName.APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_SERVICE.getEntityName(),
                                                 EntityTypeSimpleName.APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_WORKFLOW.getEntityName(),
                                                 EntityTypeSimpleName.APP));
    // using program types
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.FLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                     EntityTypeSimpleName.APP));
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.MAPREDUCE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                     EntityTypeSimpleName.APP));
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.SERVICE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                     EntityTypeSimpleName.APP));
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.WORKFLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                     EntityTypeSimpleName.APP));

    // using schedule
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, "DailySchedule", null));
  }

  private void assertProgramSearch() throws Exception {
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      searchMetadata(TEST_NAMESPACE, "batch", EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      searchMetadata(TEST_NAMESPACE, "realtime", EntityTypeSimpleName.PROGRAM));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW)
      ),
      searchMetadata(TEST_NAMESPACE, PURCHASE_FLOW.getEntityName(), EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_BUILDER.getEntityName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      ),
      searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_SERVICE.getEntityName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE)
      ),
      searchMetadata(TEST_NAMESPACE, CATALOG_LOOKUP_SERVICE.getEntityName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      searchMetadata(TEST_NAMESPACE, USER_PROFILE_SERVICE.getEntityName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      searchMetadata(TEST_NAMESPACE, PURCHASE_HISTORY_WORKFLOW.getEntityName(),
                     EntityTypeSimpleName.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW)
      ),
      searchMetadata(TEST_NAMESPACE, ProgramType.FLOW.getPrettyName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER)
      ),
      searchMetadata(TEST_NAMESPACE, ProgramType.MAPREDUCE.getPrettyName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      searchMetadata(TEST_NAMESPACE, ProgramType.SERVICE.getPrettyName(),
                     EntityTypeSimpleName.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      searchMetadata(TEST_NAMESPACE, ProgramType.WORKFLOW.getPrettyName(),
                     EntityTypeSimpleName.PROGRAM));
  }

  private void assertDataEntitySearch() throws Exception {
    StreamViewId view = PURCHASE_STREAM.view("view");

    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(PURCHASE_STREAM)
    );

    // schema search with fieldname
    Set<MetadataSearchResultRecord> result = searchMetadata(TEST_NAMESPACE, "body", null);
    Assert.assertEquals(expected, result);

    // schema search with fieldname and fieldtype
    result = searchMetadata(TEST_NAMESPACE, "body:" + Schema.Type.STRING.toString(), null);
    Assert.assertEquals(expected, result);

    // schema search for partial fieldname
    result = searchMetadata(TEST_NAMESPACE, "bo*", null);
    Assert.assertEquals(expected, result);

    // schema search with fieldname and all/partial fieldtype
    result = searchMetadata(TEST_NAMESPACE, "body:STR*", null);
    Assert.assertEquals(expected, result);

    // create a view
    Schema viewSchema = Schema.recordOf("record",
                                        Schema.Field.of("viewBody", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StreamViewClient viewClient = new StreamViewClient(getClientConfig(), getRestClient());
    viewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification(Formats.AVRO, viewSchema)));

    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    metadataClient.addProperties(HISTORY_DS, datasetProperties);

    result = searchMetadata(TEST_NAMESPACE, "schema:*", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(PURCHASE_STREAM))
                          .add(new MetadataSearchResultRecord(HISTORY_DS))
                          .add(new MetadataSearchResultRecord(PURCHASES_DS))
                          .add(new MetadataSearchResultRecord(view))
                          .build(),
                        result);

    // search dataset
    Set<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(FREQUENT_CUSTOMERS_DS), new MetadataSearchResultRecord(USER_PROFILES_DS)
    );
    Set<MetadataSearchResultRecord> expectedBatchReadables = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedKvTables)
      .add(new MetadataSearchResultRecord(PURCHASES_DS))
      .build();
    Set<MetadataSearchResultRecord> expectedAllDatasets = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedBatchReadables)
      .add(new MetadataSearchResultRecord(HISTORY_DS))
      .build();
    result = searchMetadata(TEST_NAMESPACE, "batch", EntityTypeSimpleName.DATASET);
    Assert.assertEquals(expectedAllDatasets, result);
    result = searchMetadata(TEST_NAMESPACE, "explore", EntityTypeSimpleName.DATASET);
    Assert.assertEquals(expectedAllDatasets, result);
    result = searchMetadata(TEST_NAMESPACE, KeyValueTable.class.getName(), null);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "type:*", null);
    Assert.assertEquals(expectedAllDatasets, result);

    // search using ttl
    result = searchMetadata(TEST_NAMESPACE, "ttl:*", null);
    Assert.assertEquals(expected, result);

    result = searchMetadata(TEST_NAMESPACE, PURCHASE_STREAM.getEntityName(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_STREAM),
                      new MetadataSearchResultRecord(view)
      ),
      result);

    result = searchMetadata(TEST_NAMESPACE, PURCHASE_STREAM.getEntityName(),
                            EntityTypeSimpleName.STREAM);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_STREAM)), result);
    result = searchMetadata(TEST_NAMESPACE, PURCHASE_STREAM.getEntityName(),
                            EntityTypeSimpleName.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)), result);
    result = searchMetadata(TEST_NAMESPACE, "view", EntityTypeSimpleName.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)), result);
    result = searchMetadata(TEST_NAMESPACE, HISTORY_DS.getEntityName(), null);
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(HISTORY_DS),
        new MetadataSearchResultRecord(PURCHASE_APP),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      ),
      result
    );
    // here history dataset also gets matched because it has a field called 'purchases'
    result = searchMetadata(TEST_NAMESPACE, PURCHASES_DS.getEntityName(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASES_DS),
                      new MetadataSearchResultRecord(HISTORY_DS)), result);
    result = searchMetadata(TEST_NAMESPACE, FREQUENT_CUSTOMERS_DS.getEntityName(), null);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(FREQUENT_CUSTOMERS_DS)), result);
    result = searchMetadata(TEST_NAMESPACE, USER_PROFILES_DS.getEntityName(), null);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(USER_PROFILES_DS)), result);
  }

  // starts service, makes a handler call, stops it and finally returns the runId of the completed run
  private String makePurchaseHistoryServiceCallAndReturnRunId(ServiceManager purchaseHistoryService) throws Exception {
    purchaseHistoryService.start();
    purchaseHistoryService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL serviceURL = purchaseHistoryService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL historyURL = new URL(serviceURL, "history/Milo");

    HttpResponse response = getRestClient().execute(HttpRequest.get(historyURL).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    List<RunRecord> runRecords = getRunRecords(1, getProgramClient(), PURCHASE_HISTORY_SERVICE,
                                               ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE);

    Assert.assertEquals(1, runRecords.size());
    purchaseHistoryService.stop();
    purchaseHistoryService.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    return runRecords.get(0).getPid();
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespace, String query,
                                                         EntityTypeSimpleName targetType) throws Exception {
    Set<MetadataSearchResultRecord> results =
      metadataClient.searchMetadata(namespace, query, targetType).getResults();
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
