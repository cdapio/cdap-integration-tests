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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTest extends ETLTestBase {

  private static Schema purchaseSchema = Schema.recordOf(
    "purchase",
    Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("userId", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("count", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("item", Schema.of(Schema.Type.STRING))
  );


  @Test
  public void testKVToKV() throws Exception {
    // kv table to kv table pipeline
    ETLStage source = new ETLStage("KVTableSource",
                                   new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME,
                                                                         "table1")));
    ETLStage sink = new ETLStage("KVTableSink",
                                 new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME,
                                                                       "table2")));
    ETLStage transform = new ETLStage("ProjectionTransform", new Plugin("Projection",
                                                                        ImmutableMap.<String, String>of()));
    List<ETLStage> transformList = Lists.newArrayList(transform);
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getKVTableDataset("table1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 100; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> table2 = getKVTableDataset("table2");
    KeyValueTable outputTable = table2.get();
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
    }
  }

  @Test
  public void testDAG() throws Exception {
    /*
     *             -----  highpass --------|   |--- hdfs sink (2 rows)
     *             |                       |---|
     * sourceTable ------ lowpass ---------|   |--- hbase sink (2 rows)
     * (2 rows)                       |
     *                                |------------ hdfs2 sink (1 row)
     */

    ETLStage source = new ETLStage("SourceTable", new Plugin("Table",
                                                       ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                                       Properties.Table.PROPERTY_SCHEMA_ROW_FIELD,
                                                                       "rowkey",
                                                                       Properties.Table.PROPERTY_SCHEMA,
                                                                       purchaseSchema.toString())));
    ETLStage lpFilter = new ETLStage("LowPassFilter", new Plugin(
      "ScriptFilter", ImmutableMap.of("script",
                                      "function shouldFilter(inputRecord) { return inputRecord.count > 8; }")));

    ETLStage hpFilter = new ETLStage("HighPassFilter", new Plugin(
      "ScriptFilter", ImmutableMap.of("script",
                                      "function shouldFilter(inputRecord) { return inputRecord.count < 6; }")));

    List<ETLStage> transforms = ImmutableList.of(lpFilter, hpFilter);

    ETLStage tableSink = new ETLStage("SinkTable", new Plugin(
      "Table", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "hbase",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                               Properties.Table.PROPERTY_SCHEMA, purchaseSchema.toString())));
    ETLStage hdfsSink = new ETLStage("TPFSAvro", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, purchaseSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "hdfs")));
    ETLStage hdfsSink2 = new ETLStage("TPFSAvro2", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, purchaseSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "hdfs2")));

    List<ETLStage> sinks = ImmutableList.of(tableSink, hdfsSink, hdfsSink2);
    List<Connection> connections = ImmutableList.of(
      new Connection("SourceTable", "LowPassFilter"),
      new Connection("SourceTable", "HighPassFilter"),
      new Connection("LowPassFilter", "SinkTable"),
      new Connection("HighPassFilter", "SinkTable"),
      new Connection("LowPassFilter", "TPFSAvro"),
      new Connection("HighPassFilter", "TPFSAvro"),
      new Connection("LowPassFilter", "TPFSAvro2"));

    ETLBatchConfig etlConfig = new ETLBatchConfig(ETLBatchConfig.Engine.MAPREDUCE,
                                                  "* * * * *", source, sinks, transforms, connections, null, null);

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TabToTab");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ingestPurchaseTestData(getTableDataset("input"));

    final MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mrManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 1, TimeUnit.MINUTES);

    QueryClient client = new QueryClient(getClientConfig());

    ExploreExecutionResult result = client.execute(Id.Namespace.DEFAULT, "select * from dataset_hbase")
      .get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    List<QueryResult> resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
    verifyResult("row2", resultList.get(1).getColumns());

    result = client.execute(Id.Namespace.DEFAULT, "select * from dataset_hdfs").get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
    verifyResult("row2", resultList.get(1).getColumns());

    result = client.execute(Id.Namespace.DEFAULT, "select * from dataset_hdfs2").get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(1, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTableToTableWithValidations() throws Exception {

    Schema schema = purchaseSchema;
    ETLStage source = new ETLStage("TableSource", new Plugin("Table",
                                                             ImmutableMap.of(
                                                               Properties.BatchReadableWritable.NAME, "inputTable",
                                                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                                               Properties.Table.PROPERTY_SCHEMA, schema.toString())));

    String validationScript = "function isValid(input) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.userId, 6)) " +
      "{ errCode = 10; errMsg = 'userId greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    ETLStage transform =
      new ETLStage("ValidatorTransform", new Plugin("Validator", ImmutableMap.of(
        "validators", "core", "validationScript", validationScript)), "keyErrors");
    List<ETLStage> transformList = new ArrayList<>();
    transformList.add(transform);

    ETLStage sink = new ETLStage("TableSink", new Plugin("Table",
                                                         ImmutableMap.of(
                                                           Properties.BatchReadableWritable.NAME, "outputTable",
                                                           Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey")));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TableToTable");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getTableDataset("inputTable");
    Table inputTable = inputManager.get();

    // valid record, userId "samuel" is 6 chars long
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("userId", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();

    // valid record, userId "jackson" is > 6 characters
    put = new Put(Bytes.toBytes("row2"));
    put.add("userId", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);
    inputManager.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("userId"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertEquals(0, row.getColumns().size());
  }

  private void verifyResult(String rowKey, List<Object> row) {
    Assert.assertEquals(rowKey, row.get(0));
    switch (rowKey) {
      case "row1":
        Assert.assertEquals("samuel", row.get(1));
        Assert.assertEquals(5, row.get(2));
        Assert.assertEquals(123.45, row.get(3));
        Assert.assertEquals("scotch", row.get(4));
        break;

      case "row2":
        Assert.assertEquals("jackson", row.get(1));
        Assert.assertEquals(10, row.get(2));
        Assert.assertEquals(123456789d, row.get(3));
        Assert.assertEquals("island", row.get(4));
        break;

      default:
        Assert.fail(String.format("Unrecognized rowKey {%s}!", rowKey));
    }
  }

  private void ingestPurchaseTestData(DataSetManager<Table> tableManager) {
    Table inputTable = tableManager.get();

    // valid record, userId "samuel" is 6 chars long
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("userId", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    tableManager.flush();

    // valid record, user name "jackson" is > 6 characters
    put = new Put(Bytes.toBytes("row2"));
    put.add("userId", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);

    tableManager.flush();
  }
}
