/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
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
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.cdap.plugin.common.Properties;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests GroupByAggregator
 */
public class BatchAggregatorTest extends ETLTestBase {
  public static final String SMARTWORKFLOW_NAME = SmartWorkflow.NAME;
  public static final String PURCHASE_SOURCE = "purchaseSource";
  public static final String USER_CONDITION_SOURCE = "userConditionSource";
  public static final String USER_CONDITION_SINK = "userConditionSink";
  public static final String ITEM_SINK = "itemSink";
  public static final String USER_SINK = "userSink";

  private static final List<String> CONDITIONAL_AGGREGATES = ImmutableList.of(
    "highestPrice:maxIf(price):condition(city.equals('LA'))",
    "averageDonutPrice:avgIf(price):condition(item.equals('doughnut'))",
    "totalPurchasesInTokyo:sumIf(price):condition(city.equals('Tokyo'))",
    "anyPurchaseInBerlin:anyIf(item):condition(city.equals('Berlin'))",
    "doughnutsSold:countIf(item):condition(item.equals('doughnut'))",
    "lowestPrice:minIf(price):condition(!item.equals('bagel'))"
  );

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

  private static final Schema USER_CONDITION_SCHEMA = Schema.recordOf(
    "user_condition",
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("age", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("isMember", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("city", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Schema USER_CONDITION_OUTPUT_SCHEMA = Schema.recordOf(
    "user_condition",
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("highestPrice", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("averageDonutPrice", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("totalPurchasesInTokyo", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("anyPurchaseInBerlin", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("doughnutsSold", Schema.of(Schema.Type.INT)),
    Schema.Field.of("lowestPrice", Schema.of(Schema.Type.DOUBLE))
  );

  @Test
  public void test() throws Exception {
    ETLStage purchaseStage =
      new ETLStage("purchases", new ETLPlugin("Table",
                                              BatchSource.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, PURCHASE_SOURCE,
                                                Properties.Table.PROPERTY_SCHEMA, PURCHASE_SCHEMA.toString()), null));

    ETLStage userSinkStage = getSink(USER_SINK, USER_SCHEMA);

    ETLStage itemSinkStage = getSink(ITEM_SINK, ITEM_SCHEMA);


    ETLStage userGroupStage = getGroupStage("userGroup", "user",
                                            "totalPurchases:count(*), totalSpent:sum(price)");


    ETLStage itemGroupStage = getGroupStage("itemGroup", "item",
                                            "totalPurchases:count(user), latestPurchase:max(ts)");

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
      .setDriverResources(new Resources(2048))
      .setResources(new Resources(2048))
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("groupby-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(
      SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    ingestData(PURCHASE_SOURCE);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SMARTWORKFLOW_NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 15, TimeUnit.MINUTES);

    Map<String, List<Long>> groupedUsers = readOutput(serviceManager, USER_SINK, USER_SCHEMA);
    Map<String, List<Long>> groupedItems = readOutput(serviceManager, ITEM_SINK, ITEM_SCHEMA);

    verifyOutput(groupedUsers, groupedItems);
  }

  @Test
  public void testCondition() throws Exception {
    ETLStage sourceStage =
      new ETLStage("source", new ETLPlugin("Table",
                                              BatchSource.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, USER_CONDITION_SOURCE,
                                                Properties.Table.PROPERTY_SCHEMA, USER_CONDITION_SCHEMA.toString()),
                                           null));

    ETLStage groupStage = getGroupStage("group", "name", String.join(",", CONDITIONAL_AGGREGATES));

    ETLStage sinkStage = getSink(USER_CONDITION_SINK, USER_CONDITION_OUTPUT_SCHEMA);

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(sourceStage)
      .addStage(groupStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), groupStage.getName())
      .addConnection(groupStage.getName(), sinkStage.getName())
      .setDriverResources(new Resources(2048))
      .setResources(new Resources(2048))
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("groupby-condition-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(
      SnapshotFilesetService.class.getSimpleName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    ingestConditionData(USER_CONDITION_SOURCE);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SMARTWORKFLOW_NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 15, TimeUnit.MINUTES);


    Map<String, List<Object>> groups = parseConditionOutput(serviceManager);
    verifyConditionOutput(groups);
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

  private Map<String, List<Long>> readOutput(ServiceManager serviceManager, String sink, Schema schema)
    throws IOException {
    URL pfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                         String.format("read/%s", sink));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
      response, new TypeToken<Map<String, byte[]>>() {
      }.getType()).getResponseObject();

    return parseOutput(map, schema);
  }

  private Map<String, List<Long>> parseOutput(Map<String, byte[]> contents, Schema schema) throws IOException {
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

  private Map<String, List<Object>> parseConditionOutput(ServiceManager serviceManager) throws Exception {
    URL pfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                         String.format("read/%s", USER_CONDITION_SINK));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
      response, new TypeToken<Map<String, byte[]>>() {
      }.getType()).getResponseObject();

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
      .parse(USER_CONDITION_OUTPUT_SCHEMA.toString());

    Map<String, List<Object>> group = new HashMap<>();

    for (Map.Entry<String, byte[]> entry : map.entrySet()) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      DataFileStream<GenericRecord> fileStream = new DataFileStream<>(
        new ByteArrayInputStream(entry.getValue()), datumReader);
      for (GenericRecord record : fileStream) {
        List<Schema.Field> fields = USER_CONDITION_OUTPUT_SCHEMA.getFields();
        List<Object> values = new ArrayList<>();
        values.add(record.get(fields.get(1).getName()));
        values.add(record.get(fields.get(2).getName()));
        values.add(record.get(fields.get(3).getName()));
        values.add(record.get(fields.get(4).getName()));
        values.add(record.get(fields.get(5).getName()));
        values.add(record.get(fields.get(6).getName()));

        group.put(record.get(fields.get(0).getName()).toString(), values);
      }
      fileStream.close();
    }
    return group;
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
  }

  private void verifyConditionOutput(Map<String, List<Object>> groups) {
    Assert.assertEquals(3, groups.size());

    List<Object> groupedValues = groups.get("Ben");
    Assert.assertEquals(2.05, groupedValues.get(0));
    Assert.assertEquals(1.125, groupedValues.get(1));
    Assert.assertEquals(3.25, groupedValues.get(2));
    Assert.assertEquals("doughnut", groupedValues.get(3).toString());
    Assert.assertEquals(2, groupedValues.get(4));
    Assert.assertEquals(0.75, groupedValues.get(5));

    groupedValues = groups.get("Ron");
    Assert.assertEquals(2.95, groupedValues.get(0));
    Assert.assertEquals(1.75, groupedValues.get(1));
    Assert.assertEquals(0.5, groupedValues.get(2));
    Assert.assertEquals("doughnut", groupedValues.get(3).toString());
    Assert.assertEquals(1, groupedValues.get(4));
    Assert.assertEquals(0.5, groupedValues.get(5));

    groupedValues = groups.get("Emma");
    Assert.assertEquals(2.95, groupedValues.get(0));
    Assert.assertEquals(1.75, groupedValues.get(1));
    Assert.assertEquals(1.75, groupedValues.get(2));
    Assert.assertEquals("pretzel", groupedValues.get(3).toString());
    Assert.assertEquals(1, groupedValues.get(4));
    Assert.assertEquals(1.75, groupedValues.get(5));
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

  private void putValues(Table purchaseTable, int index, long timestamp,
                         String user, String item, long price) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("ts", timestamp);
    put.add("user", user);
    put.add("item", item);
    put.add("price", price);
    purchaseTable.put(put);
  }

  private void ingestConditionData(String conditionDatasetName) throws Exception {
    DataSetManager<Table> manager = getTableDataset(conditionDatasetName);
    Table table = manager.get();
    putConditionValues(table, 1, "Ben", 23, true, "Berlin", "doughnut", 1.5);
    putConditionValues(table, 2, "Ben", 23, true, "LA", "pretzel", 2.05);
    putConditionValues(table, 3, "Ben", 23, true, "Berlin", "doughnut", 0.75);
    putConditionValues(table, 4, "Ben", 23, true, "Tokyo", "pastry", 3.25);
    putConditionValues(table, 5, "Emma", 18, false, "Tokyo", "doughnut", 1.75);
    putConditionValues(table, 6, "Emma", 18, false, "LA", "bagel", 2.95);
    putConditionValues(table, 7, "Emma", 18, false, "Berlin", "pretzel", 2.05);
    putConditionValues(table, 8, "Ron", 22, true, "LA", "bagel", 2.95);
    putConditionValues(table, 9, "Ron", 22, true, "Tokyo", "pretzel", 0.5);
    putConditionValues(table, 10, "Ron", 22, true, "Berlin", "doughnut", 1.75);

    manager.flush();
  }

  private void putConditionValues(Table table, int id, String name, double age, boolean isMember, String city,
                                  String item, double price) {
    Put put = new Put(Bytes.toBytes(id));
    put.add("name", name);
    put.add("age", age);
    put.add("isMember", isMember);
    put.add("city", city);
    put.add("item", item);
    put.add("price", price);
    table.put(put);
  }
}
