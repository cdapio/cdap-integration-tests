/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.app.etl.dataset.DatasetAccessApp;
import co.cask.cdap.app.etl.dataset.SnapshotFilesetService;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests inner join, outer join for map reduce and spark.
 */
public class BatchJoinerTest extends ETLTestBase {

  @Category({
    // Temporarily disable this test on MapR clusters till we increase the memory on cluster. Enable it
    // once as per CDAP-7421
    MapR5Incompatible.class
  })
  @Test
  public void testJoinerMR() throws Exception {
    testJoiner(Engine.MAPREDUCE);
  }

  @Category({
    // We don't support spark on these distros
    HDP20Incompatible.class,
    HDP21Incompatible.class,
    CDH51Incompatible.class,
    CDH52Incompatible.class,
    // Currently, coopr doesn't provision MapR cluster with Spark. Enable this test once COOK-108 is fixed
    MapR5Incompatible.class // MapR5x category is used for all MapR version
  })
  @Test
  public void testJoinerSpark() throws Exception {
    testJoiner(Engine.SPARK);
  }

  private void testJoiner(Engine engine) throws Exception {
    String filmDatasetName = "film-joinertest";
    String filmCategoryDatasetName = "film-category-joinertest";
    String filmActorDatasetName = "film-actor-joinertest";
    String joinedDatasetName = "joined-joinertest";

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
                                           .put(Properties.SnapshotFileSetSink.NAME, joinedDatasetName)
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
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    AppRequest<ETLBatchConfig> request =  getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("joiner-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // ingest data
    ingestToFilmTable(filmDatasetName);
    ingestToFilmActorTable(filmActorDatasetName);
    ingestToFilmCategoryTable(filmCategoryDatasetName);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 15, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(SnapshotFilesetService.class.getSimpleName());
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

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

  private Set<GenericRecord> readOutput(ServiceManager serviceManager, String sink, Schema schema)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL pfsURL = new URL(serviceManager.getServiceURL(), String.format("read/%s", sink));
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
}
