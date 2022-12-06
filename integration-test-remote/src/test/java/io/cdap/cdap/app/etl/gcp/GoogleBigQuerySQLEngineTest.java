/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.batch.DedupAggregatorTest;
import io.cdap.cdap.app.etl.batch.WindowAggregatorTest;
import io.cdap.cdap.app.etl.dataset.DatasetAccessApp;
import io.cdap.cdap.app.etl.dataset.SnapshotFilesetService;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.ETLTransformationPushdown;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.remote.dataset.AbstractDatasetApp;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.Tasks;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.suite.category.RequiresSpark;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Properties;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests inner join, outer join for BQ Pushdown
 */
public class GoogleBigQuerySQLEngineTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQuerySQLEngineTest.class);
  private static final String BQ_SQLENGINE_PLUGIN_NAME = "BigQueryPushdownEngine";
  private static final String BIG_QUERY_DATASET_PREFIX = "bq_pd_ds_";
  private static final String CONNECTION_NAME = String.format("test_bq_%s", GoogleBigQueryUtils.getUUID());
  public static final String PURCHASE_SOURCE = "purchaseSource";
  public static final String ITEM_SINK = "itemSink";
  public static final String USER_SINK = "userSink";
  public static final String USER_SOURCE = "userSource";
  private static final String WINDOW_AGGREGATOR_VERSION = "1.0.2";
  public static final long MILLISECONDS_IN_A_DAY = 24 * 60 * 60 * 1000;
  public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_SSS");

  private static BigQuery bq;
  private String bigQueryDataset;

  
  private static final Map<String, String> CONFIG_MAP_DEDUPE = new ImmutableMap.Builder<String, String>()
          .put("uniqueFields", "profession")
          .put("filterOperation", "age:Min")
          .build();
  
  private static final Map<String, String> CONFIG_MAP_WINDOW = new ImmutableMap.Builder<String, String>()
          .put("partitionFields", "profession")
          .put("aggregates", "age:first(age,1,true)")
          .build();
    
  public static final Schema PURCHASE_SCHEMA = Schema.recordOf(
          "purchase",
          Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("price", Schema.of(Schema.Type.LONG)));

  public static final Schema ITEM_SCHEMA = Schema.recordOf(
          "item",
          Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("totalPurchases", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("latestPurchase", Schema.of(Schema.Type.LONG)));

  public static final Schema USER_SCHEMA = Schema.recordOf(
          "user",
          Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("totalPurchases", Schema.of(Schema.Type.LONG)),
          Schema.Field.of("totalSpent", Schema.of(Schema.Type.LONG)));

  @BeforeClass
  public static void testClassSetup() throws IOException {
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        artifactClient.getPluginSummaries(TEST_NAMESPACE.artifact("window-aggregation",
                                                                  WINDOW_AGGREGATOR_VERSION),
          Transform.PLUGIN_TYPE);
        final ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        return GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSQLEngine.PLUGIN_TYPE, BQ_SQLENGINE_PLUGIN_NAME);
      } catch (ArtifactNotFoundException e) {
          installPluginFromHub("plugin-window-aggregation", "window-aggregator",
                               WINDOW_AGGREGATOR_VERSION);
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
    createConnection(CONNECTION_NAME, "BigQuery");
    bigQueryDataset = BIG_QUERY_DATASET_PREFIX + LocalDateTime.now().format(DATE_TIME_FORMAT);
    createDataset(bigQueryDataset);
  }

  @Override
  protected void innerTearDown() throws Exception {
    deleteDataset(bigQueryDataset);
    deleteConnection(CONNECTION_NAME);
  }

  @Category({
    RequiresSpark.class
  })
  @Test
  public void testSQLEngineJoinSpark() throws Exception {
    testSQLEngineJoin(Engine.SPARK, false);
    testSQLEngineJoin(Engine.SPARK, true);
  }

  @Category({
          RequiresSpark.class
  })
  @Test
  public void testSQLEngineGroupBySpark() throws Exception {
     testSQLEngineGroupBy(Engine.SPARK, false);
     testSQLEngineGroupBy(Engine.SPARK, true);
  }

  @Category({
          RequiresSpark.class
  })
  @Test
  public void testSQLEngineDeduplicateSpark() throws Exception {
     testSQLEngineDeduplicate(Engine.SPARK, false);
     testSQLEngineDeduplicate(Engine.SPARK, true);
  }
    
  @Category({
    RequiresSpark.class
  })
  @Test
  public void testSQLEngineWindowSpark() throws Exception {
    testSQLEngineWindow(Engine.SPARK, false);
    testSQLEngineWindow(Engine.SPARK, true);
  }
    
   private void testSQLEngineWindow(Engine engine, boolean useConnection) throws Exception {
    ETLStage userSourceStage =
      new ETLStage("users", new ETLPlugin("Table",
                                          BatchSource.PLUGIN_TYPE,
                                          ImmutableMap.of(
          Properties.BatchReadableWritable.NAME, USER_SOURCE,
          Properties.Table.PROPERTY_SCHEMA, DedupAggregatorTest.USER_SCHEMA.toString()),
                                          null));

    ETLStage userSinkStage =  new ETLStage(USER_SINK, new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                                                    ImmutableMap.<String, String>builder()
                   .put(Properties.BatchReadableWritable.NAME, USER_SINK)
                   .put("schema", WindowAggregatorTest.USER_SCHEMA.toString())
                   .build(), null));

    ArtifactSelectorConfig selectorConfig = new ArtifactSelectorConfig(null,
                                                                       "google-cloud",
                                                                       "[0.18.0-SNAPSHOT, 1.0.0-SNAPSHOT)");
    ArtifactSelectorConfig WINDOW_ARTIFACT =
      new ArtifactSelectorConfig("USER", "window-aggregator", WINDOW_AGGREGATOR_VERSION);

    ETLStage userGroupStage = new ETLStage("KeyAggregate", new ETLPlugin("WindowAggregation",
                                                                         SparkCompute.PLUGIN_TYPE,
                                                                         CONFIG_MAP_WINDOW, WINDOW_ARTIFACT));

    ETLTransformationPushdown transformationPushdown =
      new ETLTransformationPushdown(
        new ETLPlugin(BQ_SQLENGINE_PLUGIN_NAME,
                      BatchSQLEngine.PLUGIN_TYPE,
                      getProps(useConnection, "KeyAggregate"),
                      selectorConfig
        )
      );

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
                                          .addStage(userSourceStage)
                                          .addStage(userSinkStage)
                                          .addStage(userGroupStage)
                                          .addConnection(userSourceStage.getName(), userGroupStage.getName())
                                          .addConnection(userGroupStage.getName(), userSinkStage.getName())
                                          .setDriverResources(new Resources(2048))
                                          .setResources(new Resources(2048))
                                          .setEngine(engine)
                                          .setPushdownEnabled(true)
                                          .setTransformationPushdown(transformationPushdown)
                                          .build();


    ingestInputData(USER_SOURCE);

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("bq-sqlengine-windowaggregation-test");
    ApplicationManager appManager = deployApplication(appId, request);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager
                                                        (SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser()
      .parse(WindowAggregatorTest.USER_SCHEMA.toString());

    GenericRecord record1 = new GenericRecordBuilder(avroOutputSchema)
      .set("Lastname", "Schuster")
      .set("Firstname", "Chris")
      .set("profession", "accountant")
      .set("age", 23)
      .build();

    GenericRecord record2 = new GenericRecordBuilder(avroOutputSchema)
      .set("Lastname", "Bolt")
      .set("Firstname", "Henry")
      .set("profession", "engineer")
      .set("age", 30)
      .build();

    GenericRecord record3 = new GenericRecordBuilder(avroOutputSchema)
      .set("Lastname", "Seitz")
      .set("Firstname", "Bob")
      .set("profession", "professor")
      .set("age", 45)
      .build();

    GenericRecord record4 = new GenericRecordBuilder(avroOutputSchema)
      .set("Lastname", "Shelton")
      .set("Firstname", "Alex")
      .set("profession", "professor")
      .set("age", 45)
      .build();

    GenericRecord record5 = new GenericRecordBuilder(avroOutputSchema)
      .set("Lastname", "Gamal")
      .set("Firstname", "Ali")
      .set("profession", "engineer")
      .set("age", 30)
      .build();

    Set<GenericRecord> expected = ImmutableSet.of(record1, record2, record3, record4, record5);
    // verfiy output
    Assert.assertEquals(expected, readOutput(serviceManager, USER_SINK, DedupAggregatorTest.USER_SCHEMA));
    Assert.assertTrue(countTablesInDataset(bigQueryDataset) > 0);
  }  
    
  private Map<String, String> getProps(boolean useConnection, String includedStages) {
    String connectionId = String.format("${conn(%s)}", CONNECTION_NAME);
    Map<String, String> props = new HashMap<>();
    if (useConnection) {
      props.put(ConfigUtil.NAME_CONNECTION, connectionId);
      props.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    }
    props.put("dataset", bigQueryDataset);
    props.put("retainTables", "true");
    if (includedStages != null) {
      props.put("includedStages", includedStages);
    }
    return new ImmutableMap.Builder<String, String>().putAll(props).build();
  }

  private void testSQLEngineDeduplicate(Engine engine, boolean useConnection) throws Exception {
    ETLStage userSourceStage =
            new ETLStage("users", new ETLPlugin("Table",
                    BatchSource.PLUGIN_TYPE,
                    ImmutableMap.of(
                            Properties.BatchReadableWritable.NAME, USER_SOURCE,
                            Properties.Table.PROPERTY_SCHEMA, DedupAggregatorTest.USER_SCHEMA.toString()),
                    null));

    ETLStage userSinkStage =  new ETLStage(USER_SINK, new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
            ImmutableMap.<String, String>builder()
                    .put(Properties.BatchReadableWritable.NAME, USER_SINK)
                    .put("schema", DedupAggregatorTest.USER_SCHEMA.toString())
                    .build(), null));

    ArtifactSelectorConfig selectorConfig = new ArtifactSelectorConfig(null,
            "google-cloud",
            "[0.18.0-SNAPSHOT, 1.0.0-SNAPSHOT)");

    ETLStage userGroupStage = new ETLStage("KeyAggregate", new ETLPlugin("Deduplicate",
            BatchAggregator.PLUGIN_TYPE,
            CONFIG_MAP_DEDUPE, null));

    ETLTransformationPushdown transformationPushdown =
            new ETLTransformationPushdown(
                    new ETLPlugin(BQ_SQLENGINE_PLUGIN_NAME,
                            BatchSQLEngine.PLUGIN_TYPE,
                            getProps(useConnection, "KeyAggregate"),
                            selectorConfig
                    )
            );

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
            .addStage(userSourceStage)
            .addStage(userSinkStage)
            .addStage(userGroupStage)
            .addConnection(userSourceStage.getName(), userGroupStage.getName())
            .addConnection(userGroupStage.getName(), userSinkStage.getName())
            .setDriverResources(new Resources(2048))
            .setResources(new Resources(2048))
            .setEngine(engine)
            .setPushdownEnabled(true)
            .setTransformationPushdown(transformationPushdown)
            .build();


    ingestInputData(USER_SOURCE);

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("bq-sqlengine-deduplicate-test");
    ApplicationManager appManager = deployApplication(appId, request);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager
            (SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser()
            .parse(DedupAggregatorTest.USER_SCHEMA.toString());
    // output has these records:
    // 1: shelton, alex, professor, 45
    // 3: schuster, chris, accountant, 23
    // 5: gamal , ali , engineer, 28
    GenericRecord record1 = new GenericRecordBuilder(avroOutputSchema)
            .set("Lastname", "Shelton")
            .set("Firstname", "Alex")
            .set("profession", "professor")
            .set("age", 45)
            .build();

    GenericRecord record2 = new GenericRecordBuilder(avroOutputSchema)
            .set("Lastname", "Schuster")
            .set("Firstname", "Chris")
            .set("profession", "accountant")
            .set("age", 23)
            .build();

    GenericRecord record3 = new GenericRecordBuilder(avroOutputSchema)
            .set("Lastname", "Gamal")
            .set("Firstname", "Ali")
            .set("profession", "engineer")
            .set("age", 28)
            .build();

    Set<GenericRecord> expected = ImmutableSet.of(record1, record2, record3);
    // verfiy output
    Assert.assertEquals(expected, readOutput(serviceManager, USER_SINK, DedupAggregatorTest.USER_SCHEMA));
    Assert.assertTrue(countTablesInDataset(bigQueryDataset) > 0);
  }

  private void testSQLEngineJoin(Engine engine, boolean useConnection) throws Exception {
    String filmDatasetName = "film-sqlenginejoinertest";
    String filmCategoryDatasetName = "film-category-sqlenginejoinertest";
    String filmActorDatasetName = "film-actor-sqlenginejoinertest";
    String joinedDatasetName = "joined-sqlenginejoinertest";

    Schema filmSchema = Schema.recordOf(
      "film",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)));

    ETLStage filmStage =
      new ETLStage("film",
                   new ETLPlugin("Table",
                                 BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, filmDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA, filmSchema.toString()),
                                 null));


    Schema filmActorSchema = Schema.recordOf(
      "filmActor",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("actor_name", Schema.of(Schema.Type.STRING)));

    ETLStage filmActorStage =
      new ETLStage("filmActor",
                   new ETLPlugin("Table",
                                 BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, filmActorDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA, filmActorSchema.toString()),
                                 null));

    Schema filmCategorySchema = Schema.recordOf(
      "filmCategory",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("category_name", Schema.of(Schema.Type.STRING)));

    ETLStage filmCategoryStage =
      new ETLStage("filmCategory",
                   new ETLPlugin("Table",
                                 BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   Properties.BatchReadableWritable.NAME, filmCategoryDatasetName,
                                   Properties.Table.PROPERTY_SCHEMA, filmCategorySchema.toString()),
                                 null));

    String selectedFields1 = "film.film_id, film.film_name, filmActor.actor_name as renamed_actor";

    ETLStage innerJoinStage =
      new ETLStage("innerJoin",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "joinKeys", "film.film_id=filmActor.film_id&film.film_name=filmActor.film_name",
                                   "selectedFields", selectedFields1,
                                   "requiredInputs", "film,filmActor"),
                                 null));

    String selectedFields2 = "innerJoin.film_id, innerJoin.film_name, innerJoin.renamed_actor, " +
      "filmCategory.category_name as renamed_category";

    ETLStage outerJoinStage =
      new ETLStage("outerJoin",
                   new ETLPlugin("Joiner",
                                 BatchJoiner.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "joinKeys", "innerJoin.film_id=filmCategory.film_id",
                                   "selectedFields", selectedFields2,
                                   "requiredInputs", "innerJoin"),
                                 null));

    ETLTransformationPushdown transformationPushdown =
      new ETLTransformationPushdown(
        new ETLPlugin(BQ_SQLENGINE_PLUGIN_NAME,
                      BatchSQLEngine.PLUGIN_TYPE,
                      getProps(useConnection, null)
        )
      );

    // output schema sorted by input stage names
    Schema outputSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("film_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("film_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_actor", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("renamed_category", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    ETLStage joinSinkStage =
      new ETLStage("sink", new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.<String, String>builder()
                                           .put(Properties.BatchReadableWritable.NAME, joinedDatasetName)
                                           .put("schema", outputSchema.toString())
                                           .build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(filmStage)
      .addStage(filmActorStage)
      .addStage(filmCategoryStage)
      .addStage(innerJoinStage)
      .addStage(outerJoinStage)
      .addStage(joinSinkStage)
      .addConnection(filmStage.getName(), innerJoinStage.getName())
      .addConnection(filmActorStage.getName(), innerJoinStage.getName())
      .addConnection(filmCategoryStage.getName(), outerJoinStage.getName())
      .addConnection(innerJoinStage.getName(), outerJoinStage.getName())
      .addConnection(outerJoinStage.getName(), joinSinkStage.getName())
      .setEngine(engine)
      .setPushdownEnabled(true)
      .setTransformationPushdown(transformationPushdown)
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    AppRequest<ETLBatchConfig> request =  getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("bq-sqlengine-joiner-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // ingest data
    ingestToFilmTable(filmDatasetName);
    ingestToFilmActorTable(filmActorDatasetName);
    ingestToFilmCategoryTable(filmCategoryDatasetName);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 15, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    org.apache.avro.Schema avroOutputSchema = new Parser().parse(outputSchema.toString());
    GenericRecord record1 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "action")
      .set("renamed_actor", "alex")
      .build();

    GenericRecord record2 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "thriller")
      .set("renamed_actor", "alex")
      .build();

    GenericRecord record3 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "action")
      .set("renamed_actor", "bob")
      .build();

    GenericRecord record4 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "1")
      .set("film_name", "matrix")
      .set("renamed_category", "thriller")
      .set("renamed_actor", "bob")
      .build();

    GenericRecord record5 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "2")
      .set("film_name", "equilibrium")
      .set("renamed_category", "action")
      .set("renamed_actor", "cathie")
      .build();

    GenericRecord record6 = new GenericRecordBuilder(avroOutputSchema)
      .set("film_id", "3")
      .set("film_name", "avatar")
      .set("renamed_actor", "samuel")
      .set("renamed_category", null)
      .build();

    Set<GenericRecord> expected = ImmutableSet.of(record1, record2, record3, record4, record5, record6);
    // verfiy output
    Assert.assertEquals(expected, readOutput(serviceManager, joinedDatasetName, outputSchema));
  }

  private void testSQLEngineGroupBy(Engine engine, boolean useConnection) throws Exception {
    ETLStage purchaseStage =
            new ETLStage("purchases", new ETLPlugin("Table",
                    BatchSource.PLUGIN_TYPE,
                    ImmutableMap.of(
                            Properties.BatchReadableWritable.NAME, PURCHASE_SOURCE,
                            Properties.Table.PROPERTY_SCHEMA, PURCHASE_SCHEMA.toString()), null));

    ETLStage userSinkStage = getSink(USER_SINK, USER_SCHEMA);

    ETLStage itemSinkStage = getSink(ITEM_SINK, ITEM_SCHEMA);


    ETLStage userGroupStage = getGroupStage("userGroup", "user",
            "totalPurchases:count(item), totalSpent:sum(price)");


    ETLStage itemGroupStage = getGroupStage("itemGroup", "item",
            "totalPurchases:count(user), latestPurchase:max(ts)");

    List<String> includedStagesList =
            Lists.newArrayList("userGroup", "itemGroup");
    String includedStages = Joiner.on("\u0001").join(includedStagesList);

    ArtifactSelectorConfig selectorConfig = new ArtifactSelectorConfig(null,
            "google-cloud",
            "[0.18.0-SNAPSHOT, 1.0.0-SNAPSHOT)");

    ETLTransformationPushdown transformationPushdown =
            new ETLTransformationPushdown(
                    new ETLPlugin(BQ_SQLENGINE_PLUGIN_NAME,
                            BatchSQLEngine.PLUGIN_TYPE,
                            getProps(useConnection, includedStages),
                            selectorConfig
                    )
            );

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
            .addStage(purchaseStage)
            .addStage(userSinkStage)
            .addStage(itemSinkStage)
            .addStage(userGroupStage)
            .addStage(itemGroupStage)
            .addConnection(purchaseStage.getName(), userGroupStage.getName())
            .addConnection(purchaseStage.getName(), itemGroupStage.getName())
            .addConnection(userGroupStage.getName(), userSinkStage.getName())
            .addConnection(itemGroupStage.getName(), itemSinkStage.getName())
            .setEngine(engine)
            .setPushdownEnabled(true)
            .setTransformationPushdown(transformationPushdown)
            .setDriverResources(new Resources(2048))
            .setResources(new Resources(2048))
            .build();

    AppRequest<ETLBatchConfig> request =  getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("bq-sqlengine-groupby-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // ingest data
    ingestData(PURCHASE_SOURCE);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 15, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    Map<String, List<Long>> groupedUsers = readOutputGroupBy(serviceManager, USER_SINK, USER_SCHEMA);
    Map<String, List<Long>> groupedItems = readOutputGroupBy(serviceManager, ITEM_SINK, ITEM_SCHEMA);

    verifyOutput(groupedUsers, groupedItems);
  }
  
  private Map<String, List<Long>> readOutputGroupBy(ServiceManager serviceManager, String sink, Schema schema)
          throws IOException {
    URL pfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
            String.format("read/%s", sink));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
            response, new TypeToken<Map<String, byte[]>>() {
            }.getType()).getResponseObject();

    return parseOutputGroupBy(map, schema);
  }

  private Map<String, List<Long>> parseOutputGroupBy(Map<String, byte[]> contents, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    Map<String, List<Long>> group = new HashMap<>();

    for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      DataFileStream<GenericRecord> fileStream = new DataFileStream<>(
              new ByteArrayInputStream(entry.getValue()), datumReader);
      for (GenericRecord record : fileStream) {
        List<Schema.Field> fields = schema.getFields();
        List<Long> values = new ArrayList<>();
        values.add((Long) record.get(fields.get(1).getName()));
        values.add((Long) record.get(fields.get(2).getName()));
        group.put(record.get(fields.get(0).getName()).toString(), values);
      }
      fileStream.close();
    }
    return group;
  }

  private void ingestData(String purchasesDatasetName) throws Exception {
    // write input data
    // 1: 1234567890000, samuel, island, 1000
    // 2: 1234567890001, samuel, shirt, 15
    // 3. 1234567890001, samuel, pie, 20
    // 4. 1234567890002, john, pie, 25
    // 5. 1234567890003, john, shirt, 30
    DataSetManager<Table> purchaseManager = getTableDataset(purchasesDatasetName);
    Table purchaseTable = purchaseManager.get();
    // 1: 1234567890000, samuel, island, 1000
    putValues(purchaseTable, 1, 1234567890000L, "samuel", "island", 1000L);
    putValues(purchaseTable, 2, 1234567890001L, "samuel", "shirt", 15L);
    putValues(purchaseTable, 3, 1234567890001L, "samuel", "pie", 20L);
    putValues(purchaseTable, 4, 1234567890002L, "john", "pie", 25L);
    putValues(purchaseTable, 5, 1234567890003L, "john", "shirt", 30L);
    purchaseManager.flush();
  }

  private void ingestInputData(String inputDatasetName) throws Exception {
    // 1: shelton, alex, professor, 45
    // 2: seitz, bob, professor, 50
    // 3: schuster, chris, accountant, 23
    // 4: bolt , henry , engineer, 30
    // 5: gamal , ali , engineer, 28
    DataSetManager<Table> inputManager = getTableDataset(inputDatasetName);
    Table inputTable = inputManager.get();
    putValues(inputTable, 1, "Shelton", "Alex", "professor",  45);
    putValues(inputTable, 2, "Seitz", "Bob", "professor",  50);
    putValues(inputTable, 3, "Schuster", "Chris", "accountant",  23);
    putValues(inputTable, 4, "Bolt", "Henry", "engineer",  30);
    putValues(inputTable, 5, "Gamal", "Ali", "engineer",  28);
    inputManager.flush();
  }

  private void putValues(Table inputTable, int index, String lastname, String firstname, String profession,
                         int age) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("Lastname", lastname);
    put.add("Firstname", firstname);
    put.add("profession", profession);
    put.add("age", age);
    inputTable.put(put);
  }

  private void putValues(Table purchaseTable, int index, long timestamp,
                         String user, String item, long price) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("ts", timestamp);
    put.add("user", user);
    put.add("item", item);
    put.add("price", price);
    purchaseTable.put(put);
  }

  private void verifyOutput(Map<String, List<Long>> groupedUsers, Map<String, List<Long>> groupedItems) {
    // users table should have:
    // samuel: 3, 1000 + 15 + 20
    List<Long> groupedValues = groupedUsers.get("samuel");
    Assert.assertEquals(groupedValues.get(0).longValue(), 3L);
    Assert.assertEquals(Math.abs(groupedValues.get(1).longValue() - 1000L - 15L - 20L), 0L);
    // john: 2, 25 + 30
    groupedValues = groupedUsers.get("john");
    Assert.assertEquals(groupedValues.get(0).longValue(), 2L);
    Assert.assertEquals(Math.abs(groupedValues.get(1).longValue() - 25L - 30L), 0L);

    // items table should have:
    // island: 1, 1234567890000
    groupedValues = groupedItems.get("island");
    Assert.assertEquals(groupedValues.get(0).longValue(), 1L);
    Assert.assertEquals(groupedValues.get(1).longValue(), 1234567890000L);

    // pie: 2, 1234567890002
    groupedValues = groupedItems.get("pie");
    Assert.assertEquals(groupedValues.get(0).longValue(), 2L);
    Assert.assertEquals(groupedValues.get(1).longValue(), 1234567890002L);

    // shirt: 2, 1234567890003
    groupedValues = groupedItems.get("shirt");
    Assert.assertEquals(groupedValues.get(0).longValue(), 2L);
    Assert.assertEquals(groupedValues.get(1).longValue(), 1234567890003L);

    Assert.assertTrue(countTablesInDataset(bigQueryDataset) > 0);
  }

  private ETLStage getSink(String name, Schema schema) {
    return new ETLStage(name, new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
            ImmutableMap.<String, String>builder()
                    .put(Properties.BatchReadableWritable.NAME, name)
                    .put("schema", schema.toString())
                    .build(), null));
  }

  private ETLStage getGroupStage(String name, String field, String condition) {
    return new ETLStage(name,
            new ETLPlugin("GroupByAggregate",
                    BatchAggregator.PLUGIN_TYPE,
                    ImmutableMap.of(
                            "groupByFields", field,
                            "aggregates", condition), null));
  }

  private Set<GenericRecord> readOutput(ServiceManager serviceManager, String sink, Schema schema)
    throws IOException {
    URL pfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                         String.format("read/%s", sink));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
      response, new TypeToken<Map<String, byte[]>>() { }.getType()).getResponseObject();

    return parseOutput(map, schema);
  }

  private Set<GenericRecord> parseOutput(Map<String, byte[]> contents, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new Parser().parse(schema.toString());
    Set<GenericRecord> records = new HashSet<>();
    for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      try (DataFileStream<GenericRecord> fileStream = new DataFileStream<>(
        new ByteArrayInputStream(entry.getValue()), datumReader)) {
        for (GenericRecord record : fileStream) {
          records.add(record);
        }
      }
    }
    return records;
  }

  private void ingestToFilmCategoryTable(String filmCategoryDatasetName) throws Exception {
    // 1: 1, matrix, action
    // 2: 1, matrix, thriller
    // 3: 2, equilibrium, action
    DataSetManager<Table> filmCategoryManager = getTableDataset(filmCategoryDatasetName);
    Table filmCategoryTable = filmCategoryManager.get();
    putFilmCategory(filmCategoryTable, 1, "1", "matrix", "action");
    putFilmCategory(filmCategoryTable, 2, "1", "matrix", "thriller");
    putFilmCategory(filmCategoryTable, 3, "2", "equilibrium", "action");
    filmCategoryManager.flush();
    stopServiceForDataset(filmCategoryDatasetName);
  }

  private void putFilmCategory(Table table, int id, String filmId, String filmName, String categoryName) {
    Put put = new Put(Bytes.toBytes(id));
    put.add("film_id", filmId);
    put.add("film_name", filmName);
    put.add("category_name", categoryName);
    table.put(put);
  }

  private void ingestToFilmActorTable(String filmActorDatasetName) throws Exception {
    // 1: 1, matrix, alex
    // 2: 1, matrix, bob
    // 3: 2, equilibrium, cathie
    // 4: 3, avatar, samuel
    DataSetManager<Table> filmActorManager = getTableDataset(filmActorDatasetName);
    Table filmActorTable = filmActorManager.get();
    putFilmActor(filmActorTable, 1, "1", "matrix", "alex");
    putFilmActor(filmActorTable, 2, "1", "matrix", "bob");
    putFilmActor(filmActorTable, 3, "2", "equilibrium", "cathie");
    putFilmActor(filmActorTable, 4, "3", "avatar", "samuel");
    filmActorManager.flush();
    stopServiceForDataset(filmActorDatasetName);
  }

  private void putFilmActor(Table table, int id, String filmId, String filmName, String actorName) {
    Put put = new Put(Bytes.toBytes(id));
    put.add("film_id", filmId);
    put.add("film_name", filmName);
    put.add("actor_name", actorName);
    table.put(put);
  }

  private void ingestToFilmTable(String filmDatasetName) throws Exception {
    // write input data
    // 1: 1, matrix
    // 2: 2, equilibrium
    // 3: 3, avatar
    // 4: 4, humtum
    DataSetManager<Table> filmManager = getTableDataset(filmDatasetName);
    Table filmTable = filmManager.get();
    putFilm(filmTable, 1, "1", "matrix");
    putFilm(filmTable, 2, "2", "equilibrium");
    putFilm(filmTable, 3, "3", "avatar");
    putFilm(filmTable, 4, "4", "humtum");
    filmManager.flush();
    stopServiceForDataset(filmDatasetName);
  }

  private void putFilm(Table table, int id, String filmId, String filmName) {
    Put put = new Put(Bytes.toBytes(id));
    put.add("film_id", filmId);
    put.add("film_name", filmName);
    table.put(put);
  }

  // once we no longer need a service to interact with a dataset, can stop it to reduce resource usage
  private void stopServiceForDataset(String datasetName) throws Exception {
    getApplicationManager(TEST_NAMESPACE.app(datasetName))
      .getServiceManager(AbstractDatasetApp.DatasetService.class.getSimpleName())
      .stop();
  }

  private static void createDataset(String bigQueryDataset) {
    LOG.info("Creating bigquery dataset {}", bigQueryDataset);
    // Create dataset with a default table expiration of 24 hours.
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDataset)
            .setDefaultTableLifetime(MILLISECONDS_IN_A_DAY)
            .setDefaultPartitionExpirationMs(MILLISECONDS_IN_A_DAY)
            .build();
    bq.create(datasetInfo);
    LOG.info("Created bigquery dataset {}", bigQueryDataset);
  }

  private static void deleteDataset(String bigQueryDataset) {
    LOG.info("Deleting bigquery dataset {}", bigQueryDataset);
    boolean deleted = bq.delete(bigQueryDataset, BigQuery.DatasetDeleteOption.deleteContents());
    if (deleted) {
      LOG.info("Deleted bigquery dataset {}", bigQueryDataset);
    }
  }

  private static int countTablesInDataset(String bigQueryDataset) {
    int numTables = 0;

    // count all tables in this dataset.
    Page<com.google.cloud.bigquery.Table> page = bq.listTables(bigQueryDataset);
    for (com.google.cloud.bigquery.Table table : page.iterateAll()) {
      numTables++;
    }

    return numTables;
  }
}
