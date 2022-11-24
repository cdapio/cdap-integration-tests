/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.app.etl.dataset.DatasetAccessApp;
import io.cdap.cdap.app.etl.dataset.SnapshotFilesetService;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.suite.category.RequiresSpark;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.cdap.plugin.common.Properties;
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
 *   Tests WindowAggregator
 */
public class WindowAggregatorTest extends ETLTestBase {
  public static final String SMARTWORKFLOW_NAME = SmartWorkflow.NAME;
  public static final String USER_SOURCE = "userSource";
  public static final String USER_SINK = "userSink";

  public static final Schema USER_SCHEMA = Schema.recordOf(
    "user",
    Schema.Field.of("Lastname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Firstname", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("profession", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("age", Schema.of(Schema.Type.INT)));

  private static final Map<String, String> CONFIG_MAP_WINDOW = new ImmutableMap.Builder<String, String>()
    .put("partitionFields", "profession")
    .put("aggregates", "age:Rank")
    .build();

  @Category({
    RequiresSpark.class
  })
  @Test
  public void testWindowSpark() throws Exception {
    testWindow(Engine.SPARK);
  }

  private void testWindow(Engine spark) throws  Exception {
    ETLStage userSourceStage =
      new ETLStage("users", new ETLPlugin("Table",
                                          BatchSource.PLUGIN_TYPE,
                                          ImmutableMap.of(
                                            Properties.BatchReadableWritable.NAME, USER_SOURCE,
                                            Properties.Table.PROPERTY_SCHEMA, USER_SCHEMA.toString()), null));

    ETLStage userSinkStage =  new ETLStage(USER_SINK, new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                                                    ImmutableMap.<String, String>builder()
                                                                                .put(Properties.BatchReadableWritable.NAME, USER_SINK)
                                                                                .put("schema", USER_SCHEMA.toString())
                                                                                .build(), null));

    ETLStage userGroupStage = new ETLStage("KeyAggregate", new ETLPlugin("Window",
                                                                         SparkCompute.PLUGIN_TYPE,
                                                                         CONFIG_MAP_WINDOW, null));


    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
                                          .addStage(userSourceStage)
                                          .addStage(userSinkStage)
                                          .addStage(userGroupStage)
                                          .addConnection(userSourceStage.getName(), userGroupStage.getName())
                                          .addConnection(userGroupStage.getName(), userSinkStage.getName())
                                          .setDriverResources(new Resources(2048))
                                          .setResources(new Resources(2048))
                                          .build();


    ingestInputData(USER_SOURCE);

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("window-test");
    ApplicationManager appManager = deployApplication(appId, request);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager
                                                        (SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(USER_SCHEMA.toString());
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
    Assert.assertEquals(expected, readOutput(serviceManager, USER_SINK, USER_SCHEMA));
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
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
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
}
