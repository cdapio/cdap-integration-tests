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
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
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
  public static final String SMARTWORKFLOW_NAME = "DataPipelineWorkflow";
  public static final String PURCHASE_SOURCE = "purchaseSource";
  public static final String ITEM_SINK = "itemSink";
  public static final String USER_SINK = "userSink";

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

  @Test
  public void test() throws Exception {
    ETLStage purchaseStage =
      new ETLStage("purchases", new ETLPlugin("Table",
                                              BatchSource.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, PURCHASE_SOURCE,
                                                Properties.Table.PROPERTY_SCHEMA, PURCHASE_SCHEMA.toString()), null));

    ETLStage userSinkStage =
      new ETLStage("users", new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                          ImmutableMap.<String, String>builder()
                                            .put(Properties.SnapshotFileSetSink.NAME, USER_SINK)
                                            .put("schema", USER_SCHEMA.toString())
                                            .build(), null));

    ETLStage itemSinkStage =
      new ETLStage("items", new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                          ImmutableMap.<String, String>builder()
                                            .put(Properties.SnapshotFileSetSink.NAME, ITEM_SINK)
                                            .put("schema", ITEM_SCHEMA.toString())
                                            .build(), null));

    ETLStage userGroupStage =
      new ETLStage("userGroup",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "groupByFields", "user",
                                   "aggregates", "totalPurchases:count(*), totalSpent:sum(price)"), null));

    ETLStage itemGroupStage =
      new ETLStage("itemGroup",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of(
                                   "groupByFields", "item",
                                   "aggregates", "totalPurchases:count(user), latestPurchase:max(ts)"), null));

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
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "groupby-test");
    ApplicationManager appManager = deployApplication(appId, request);

    // Deploy an application with a service to get partitionedFileset data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(
      SnapshotFilesetService.class.getSimpleName());
    serviceManager.start();

    ingestData(PURCHASE_SOURCE);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SMARTWORKFLOW_NAME);
    workflowManager.start();
    workflowManager.waitForFinish(15, TimeUnit.MINUTES);

    Map<String, List<Long>> groupedUsers = readOutput(serviceManager, USER_SINK);
    Map<String, List<Long>> groupedItems = readOutput(serviceManager, ITEM_SINK);

    verifyOutput(groupedUsers, groupedItems);
  }

  private Map<String, List<Long>> readOutput(ServiceManager serviceManager, String sink)
    throws IOException, UnauthenticatedException {
    URL pfsURL = new URL(serviceManager.getServiceURL(), String.format("read/%s", sink));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
      response, new TypeToken<Map<String, byte[]>>() {
      }.getType()).getResponseObject();

    Schema schema = (sink.equalsIgnoreCase(USER_SINK)) ? USER_SCHEMA : ITEM_SCHEMA;
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
    putValues(purchaseManager, purchaseTable, 1, 1234567890000L, "samuel", "island", 1000L);
    putValues(purchaseManager, purchaseTable, 2, 1234567890001L, "samuel", "shirt", 15L);
    putValues(purchaseManager, purchaseTable, 3, 1234567890001L, "samuel", "pie", 20L);
    putValues(purchaseManager, purchaseTable, 4, 1234567890002L, "john", "pie", 25L);
    putValues(purchaseManager, purchaseTable, 5, 1234567890003L, "john", "shirt", 30L);
    purchaseManager.flush();
  }

  private void putValues(DataSetManager<Table> purchaseManager, Table purchaseTable, int index, long timestamp,
                         String user, String item, long price) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("ts", timestamp);
    put.add("user", user);
    put.add("item", item);
    put.add("price", price);
    purchaseTable.put(put);
  }
}
