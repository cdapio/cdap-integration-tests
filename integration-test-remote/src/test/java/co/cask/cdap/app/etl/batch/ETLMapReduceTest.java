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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    ETLStage source = new ETLStage("SourceTable",
                                   new Plugin("Table",
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
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

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSinks(sinks)
      .addTransforms(transforms)
      .addConnections(connections)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TabToTab");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ingestPurchaseTestData(getTableDataset("input"));

    final MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

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

  @Test(expected =  IOException.class)
  public void testDAGInvalidSchema() throws Exception {

    /*                                  |----- userRewardsSink (TPFS)
     *                                  |
     *                                  |
     *             -----  userRewards <rowkey, userid, rewards> --| projection <rowKey, rewards> ----
     *             |                                                                                  |---rewardsSink
     * sourceTable ------ itemRewards <rowkey, item, rewards> ----------------(invalid)---------------
     * (2 rows)                       |
     *                                |------------ itemRewardsSink (TPFS)
     */

    ETLStage source = new ETLStage("SourceTable",
                                   new Plugin("Table",
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD,
                                                              "rowkey",
                                                              Properties.Table.PROPERTY_SCHEMA,
                                                              purchaseSchema.toString())));
    Schema userRewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userId", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );
    Schema itemRewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );
    ETLStage userRewards = new ETLStage("userRewardsTransform", new Plugin(
      "JavaScript", ImmutableMap.of("script",
                                    "function transform(input, emitter, context) " +
                                      "{ emitter.emit({" +
                                      "'rowkey' : input.rowkey," +
                                      "'userId' : input.userId," +
                                      "'rewards' : 10" +
                                      "}); }",
                                    "schema", userRewardsSchema.toString()
    )));
    ETLStage itemRewards = new ETLStage("itemRewardsTransform", new Plugin(
      "JavaScript", ImmutableMap.of("script",
                                    "function transform(input, emitter, context) " +
                                      "{ emitter.emit({" +
                                      "'rowkey' : input.rowkey," +
                                      "'item' : input.item," +
                                      "'rewards' : 5" +
                                      "}); }",
                                    "schema", itemRewardsSchema.toString()
    )));

    ETLStage userDropProjection = new ETLStage("userProjection", new Plugin("Projection",
                                                                            ImmutableMap.of("drop", "userId")));
    List<ETLStage> transforms = ImmutableList.of(userRewards, itemRewards, userDropProjection);
    Schema rewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );

    ETLStage allRewardsSink = new ETLStage("allRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, rewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "allRewards")));

    ETLStage userRewardsSink = new ETLStage("userRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, userRewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "userRewards")));

    ETLStage itemRewardsSink = new ETLStage("itemRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, itemRewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "itemRewards")));

    List<ETLStage> sinks = ImmutableList.of(allRewardsSink, userRewardsSink, itemRewardsSink);

    List<Connection> connections = ImmutableList.of(
      new Connection("SourceTable", "userRewardsTransform"),
      new Connection("SourceTable", "itemRewardsTransform"),
      new Connection("userRewardsTransform", "userProjection"),
      new Connection("userProjection", "allRewards"),
      new Connection("itemRewardsTransform", "allRewards"),
      new Connection("userRewardsTransform", "userRewards"),
      new Connection("itemRewardsTransform", "itemRewards")
    );

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSinks(sinks)
      .addTransforms(transforms)
      .addConnections(connections)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TabToTab");
    // deploy would fail
    deployApplication(appId, appRequest);
  }

  @Test
  public void testDAGSchemaChanges() throws Exception {

    /*                                  |----- userRewardsSink (TPFS)(2 rows)
     *                                  |
     *                                  |
     *             -----  userRewards <rowkey, userid, rewards> --| projection <rowKey, rewards> ----
     *             |                                                                                  |---rewardsSink
     * sourceTable ------ itemRewards <rowkey, item, rewards> -----| projection <rowKey, rewards> ---      TPFS(4 rows)
     * (2 rows)                       |
     *                                |------------ itemRewardsSink (TPFS) (2 row)
     */

    ETLStage source = new ETLStage("SourceTable",
                                   new Plugin("Table",
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD,
                                                              "rowkey",
                                                              Properties.Table.PROPERTY_SCHEMA,
                                                              purchaseSchema.toString())));
    Schema userRewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userId", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );
    Schema itemRewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );
    ETLStage userRewards = new ETLStage("userRewardsTransform", new Plugin(
      "JavaScript", ImmutableMap.of("script",
                                    "function transform(input, emitter, context) " +
                                      "{ emitter.emit({" +
                                      "'rowkey' : input.rowkey," +
                                      "'userId' : input.userId," +
                                      "'rewards' : 10" +
                                      "}); }",
                                    "schema", userRewardsSchema.toString()
    )));

    ETLStage itemRewards = new ETLStage("itemRewardsTransform", new Plugin(
      "JavaScript", ImmutableMap.of("script",
                                    "function transform(input, emitter, context) " +
                                      "{ emitter.emit({" +
                                      "'rowkey' : input.rowkey," +
                                      "'item' : input.item," +
                                      "'rewards' : 5" +
                                      "}); }",
                                    "schema", itemRewardsSchema.toString()
    )));
    ETLStage userDropProjection = new ETLStage("userProjection", new Plugin("Projection",
                                                                            ImmutableMap.of("drop", "userId")));
    ETLStage itemDropProjection = new ETLStage("itemProjection", new Plugin("Projection",
                                                                            ImmutableMap.of("drop", "item")));
    List<ETLStage> transforms = ImmutableList.of(userRewards, itemRewards, userDropProjection, itemDropProjection);

    Schema rewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );

    ETLStage allRewardsSink = new ETLStage("allRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, rewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "allRewards")));

    ETLStage userRewardsSink = new ETLStage("userRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, userRewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "userRewards")));

    ETLStage itemRewardsSink = new ETLStage("itemRewards", new Plugin(
      "TPFSAvro", ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, itemRewardsSchema.toString(),
                                  Properties.TimePartitionedFileSetDataset.TPFS_NAME, "itemRewards")));

    List<ETLStage> sinks = ImmutableList.of(allRewardsSink, userRewardsSink, itemRewardsSink);

    List<Connection> connections = ImmutableList.of(
      new Connection("SourceTable", "userRewardsTransform"),
      new Connection("SourceTable", "itemRewardsTransform"),
      new Connection("userRewardsTransform", "userProjection"),
      new Connection("itemRewardsTransform", "itemProjection"),
      new Connection("userProjection", "allRewards"),
      new Connection("itemProjection", "allRewards"),
      new Connection("userRewardsTransform", "userRewards"),
      new Connection("itemRewardsTransform", "itemRewards")
    );

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .setSource(source)
      .addSinks(sinks)
      .addTransforms(transforms)
      .addConnections(connections)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TabToTab");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ingestPurchaseTestData(getTableDataset("input"));

    final MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    ExploreExecutionResult result = retryQueryExecutionTillFinished(Id.Namespace.DEFAULT,
                                                                    "select * from dataset_allRewards", 5);
    List<QueryResult> resultList = Lists.newArrayList(result);
    Assert.assertEquals(4, resultList.size());

    Set<Rewards> rewards = new HashSet();
    rewards.add(new Rewards("row1", 5));
    rewards.add(new Rewards("row1", 10));
    rewards.add(new Rewards("row2", 5));
    rewards.add(new Rewards("row2", 10));
    verifyRewardResults(resultList, rewards, "rewards");

    result = retryQueryExecutionTillFinished(Id.Namespace.DEFAULT, "select * from dataset_userRewards", 5);
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    rewards = new HashSet();
    rewards.add(new Rewards("row1", null, "samuel", 10));
    rewards.add(new Rewards("row2", null, "jackson", 10));
    verifyRewardResults(resultList, rewards, "user");

    result = retryQueryExecutionTillFinished(Id.Namespace.DEFAULT, "select * from dataset_itemRewards", 5);
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    rewards = new HashSet();
    rewards.add(new Rewards("row1", "scotch", null, 5));
    rewards.add(new Rewards("row2", "island", null, 5));
    verifyRewardResults(resultList, rewards, "item");
  }

  // have noticed errors with hivemetastore for table not found,
  // which didn't appear on consecutive run, so adding retry logic for query execute
  private ExploreExecutionResult retryQueryExecutionTillFinished(Id.Namespace namespace, String query, int retryCount)
    throws InterruptedException, ExecutionException, TimeoutException {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult result = null;
    for (int i = 0; i < retryCount; i++) {
      result = client.execute(namespace, query).get(5, TimeUnit.MINUTES);
      if (QueryStatus.OpStatus.FINISHED.equals(result.getStatus().getStatus())) {
        return result;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertNotNull(result);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    return result;
  }

  // internal test class used for verifying results, used by testDAGSchemaChanges.
  private class Rewards {
    private String rowKey;
    private String userId;
    private String item;
    private int rewards;

    private Rewards(String rowKey, String item, String userId, int rewards) {
      this.rowKey = rowKey;
      this.rewards = rewards;
      this.userId = userId;
      this.item = item;
    }

    private Rewards(String rowKey, int rewards) {
      this(rowKey, null, null, rewards);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        Rewards that = (Rewards) o;
        return Objects.equals(this.rowKey, that.rowKey) && Objects.equals(this.rewards, that.rewards) &&
          Objects.equals(this.item, that.item) && Objects.equals(this.userId, that.userId);
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.rowKey, this.userId, this.item, this.rewards});
    }

    public String toString() {
      return String.format("Rowkey : %s, userId : %s, item : %s, rewards : %s ", rowKey, userId, item, rewards);
    }
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

  private void verifyRewardResults(List<QueryResult> queryResults, Set<Rewards> expected, String type) {
    Set<Rewards> actual = new HashSet<>();
    for (QueryResult queryResult : queryResults) {
      Rewards rewards = null;
      switch (type) {
        case "rewards":
          rewards = new Rewards((String) queryResult.getColumns().get(0),
                                (Integer) (queryResult.getColumns().get(1)));
          break;
        case "user":
          rewards = new Rewards((String) queryResult.getColumns().get(0), null,
                                (String) (queryResult.getColumns().get(1)),
                                (Integer) (queryResult.getColumns().get(2)));
          break;
        case "item":
          rewards = new Rewards((String) queryResult.getColumns().get(0), (String) (queryResult.getColumns().get(1)),
                                null,
                                (Integer) (queryResult.getColumns().get(2)));
          break;
        default:
          Assert.fail(String.format("Unrecognized type {%s}!", type));
      }
      actual.add(rewards);
    }
    Assert.assertEquals(expected, actual);
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
