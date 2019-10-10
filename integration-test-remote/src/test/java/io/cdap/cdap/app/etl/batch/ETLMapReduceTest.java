/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import com.google.common.collect.Lists;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.QueryResult;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Properties;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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
                                   new ETLPlugin("KVTable",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"),
                                                 null));
    ETLStage sink = new ETLStage("KVTableSink",
                                 new ETLPlugin("KVTable",
                                               BatchSink.PLUGIN_TYPE,
                                               ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"),
                                               null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("KVToKV");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getKVTableDataset("table1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 100; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
                                   new ETLPlugin("Table",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                                 Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                                                 Properties.Table.PROPERTY_SCHEMA,
                                                                 purchaseSchema.toString()),
                                                 null));
    ETLStage lpFilter = new ETLStage("LowPassFilter", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) {" +
                        "  if (input.count <=8) {" +
                        "    emitter.emit(input);" +
                        "  }" +
                        "}"),
      null));

    ETLStage hpFilter = new ETLStage("HighPassFilter", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) {" +
                        "  if (input.count >=6) {" +
                        "    emitter.emit(input);" +
                        "  }" +
                        "}"),
      null));

    ETLStage tableSink = new ETLStage("SinkTable", new ETLPlugin(
      "Table", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.BatchReadableWritable.NAME, "hbase",
                      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                      Properties.Table.PROPERTY_SCHEMA, purchaseSchema.toString()),
      null));
    ETLStage hdfsSink = new ETLStage("TPFSAvro", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, purchaseSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "hdfs"),
      null));
    ETLStage hdfsSink2 = new ETLStage("TPFSAvro2", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, purchaseSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "hdfs2"),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(tableSink)
      .addStage(hdfsSink)
      .addStage(hdfsSink2)
      .addStage(lpFilter)
      .addStage(hpFilter)
      .addConnection(source.getName(), lpFilter.getName())
      .addConnection(source.getName(), hpFilter.getName())
      .addConnection(lpFilter.getName(), tableSink.getName())
      .addConnection(hpFilter.getName(), tableSink.getName())
      .addConnection(lpFilter.getName(), hdfsSink.getName())
      .addConnection(hpFilter.getName(), hdfsSink.getName())
      .addConnection(lpFilter.getName(), hdfsSink2.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("TabToTab");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ingestPurchaseTestData(getTableDataset("input"));

    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    QueryClient client = new QueryClient(getClientConfig());

    ExploreExecutionResult result = client.execute(TEST_NAMESPACE, "select * from dataset_hbase")
      .get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    List<QueryResult> resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
    verifyResult("row2", resultList.get(1).getColumns());

    result = client.execute(TEST_NAMESPACE, "select * from dataset_hdfs").get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
    verifyResult("row2", resultList.get(1).getColumns());

    result = client.execute(TEST_NAMESPACE, "select * from dataset_hdfs2").get(5, TimeUnit.MINUTES);
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(1, resultList.size());
    verifyResult("row1", resultList.get(0).getColumns());
  }

  @Test(expected = IOException.class)
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
                                   new ETLPlugin("Table",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                                 Properties.Table.PROPERTY_SCHEMA_ROW_FIELD,
                                                                 "rowkey",
                                                                 Properties.Table.PROPERTY_SCHEMA,
                                                                 purchaseSchema.toString()),
                                                 null));
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
    ETLStage userRewards = new ETLStage("userRewardsTransform", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) " +
                        "{ emitter.emit({" +
                        "'rowkey' : input.rowkey," +
                        "'userId' : input.userId," +
                        "'rewards' : 10" +
                        "}); }",
                      "schema", userRewardsSchema.toString()
      ),
      null));
    ETLStage itemRewards = new ETLStage("itemRewardsTransform", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) " +
                        "{ emitter.emit({" +
                        "'rowkey' : input.rowkey," +
                        "'item' : input.item," +
                        "'rewards' : 5" +
                        "}); }",
                      "schema", itemRewardsSchema.toString()
      ),
      null));

    ETLStage userDropProjection = new ETLStage("userProjection",
                                               new ETLPlugin("Projection",
                                                             Transform.PLUGIN_TYPE,
                                                             ImmutableMap.of("drop", "userId"),
                                                             null));
    Schema rewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );

    ETLStage allRewardsSink = new ETLStage("allRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, rewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "allRewards"),
      null));

    ETLStage userRewardsSink = new ETLStage("userRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, userRewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "userRewards"),
      null));

    ETLStage itemRewardsSink = new ETLStage("itemRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, itemRewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "itemRewards"),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(userRewards)
      .addStage(itemRewards)
      .addStage(userDropProjection)
      .addStage(allRewardsSink)
      .addStage(userRewardsSink)
      .addStage(itemRewardsSink)
      .addConnection(source.getName(), userRewards.getName())
      .addConnection(source.getName(), itemRewards.getName())
      .addConnection(userRewards.getName(), userDropProjection.getName())
      .addConnection(userDropProjection.getName(), allRewardsSink.getName())
      .addConnection(itemRewards.getName(), allRewardsSink.getName())
      .addConnection(userRewards.getName(), userRewardsSink.getName())
      .addConnection(itemRewards.getName(), itemRewardsSink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("TabToTab");
    // deploy should fail
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
                                   new ETLPlugin("Table",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                                 Properties.Table.PROPERTY_SCHEMA_ROW_FIELD,
                                                                 "rowkey",
                                                                 Properties.Table.PROPERTY_SCHEMA,
                                                                 purchaseSchema.toString()),
                                                 null));
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
    ETLStage userRewards = new ETLStage("userRewardsTransform", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) " +
                        "{ emitter.emit({" +
                        "'rowkey' : input.rowkey," +
                        "'userId' : input.userId," +
                        "'rewards' : 10" +
                        "}); }",
                      "schema", userRewardsSchema.toString()),
      null));

    ETLStage itemRewards = new ETLStage("itemRewardsTransform", new ETLPlugin(
      "JavaScript", Transform.PLUGIN_TYPE,
      ImmutableMap.of("script",
                      "function transform(input, emitter, context) " +
                        "{ emitter.emit({" +
                        "'rowkey' : input.rowkey," +
                        "'item' : input.item," +
                        "'rewards' : 5" +
                        "}); }",
                      "schema", itemRewardsSchema.toString()),
      null));
    ETLStage userDropProjection = new ETLStage("userProjection", new ETLPlugin("Projection",
                                                                               Transform.PLUGIN_TYPE,
                                                                               ImmutableMap.of("drop", "userId"),
                                                                               null));
    ETLStage itemDropProjection = new ETLStage("itemProjection", new ETLPlugin("Projection",
                                                                               Transform.PLUGIN_TYPE,
                                                                               ImmutableMap.of("drop", "item"),
                                                                               null));

    Schema rewardsSchema = Schema.recordOf(
      "rewards",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("rewards", Schema.of(Schema.Type.INT))
    );

    ETLStage allRewardsSink = new ETLStage("allRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, rewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "allRewards"),
      null));

    ETLStage userRewardsSink = new ETLStage("userRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, userRewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "userRewards"),
      null));

    ETLStage itemRewardsSink = new ETLStage("itemRewards", new ETLPlugin(
      "TPFSAvro", BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, itemRewardsSchema.toString(),
                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "itemRewards"),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(userRewards)
      .addStage(itemRewards)
      .addStage(userDropProjection)
      .addStage(itemDropProjection)
      .addStage(allRewardsSink)
      .addStage(userRewardsSink)
      .addStage(itemRewardsSink)
      .addConnection(source.getName(), userRewards.getName())
      .addConnection(source.getName(), itemRewards.getName())
      .addConnection(userRewards.getName(), userDropProjection.getName())
      .addConnection(itemRewards.getName(), itemDropProjection.getName())
      .addConnection(userDropProjection.getName(), allRewardsSink.getName())
      .addConnection(itemDropProjection.getName(), allRewardsSink.getName())
      .addConnection(userRewards.getName(), userRewardsSink.getName())
      .addConnection(itemRewards.getName(), itemRewardsSink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("TabToTab");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ingestPurchaseTestData(getTableDataset("input"));

    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    ExploreExecutionResult result = retryQueryExecutionTillFinished(TEST_NAMESPACE,
                                                                    "select * from dataset_allRewards", 5);
    List<QueryResult> resultList = Lists.newArrayList(result);
    Assert.assertEquals(4, resultList.size());

    Set<Rewards> rewards = new HashSet<>();
    rewards.add(new Rewards("row1", 5));
    rewards.add(new Rewards("row1", 10));
    rewards.add(new Rewards("row2", 5));
    rewards.add(new Rewards("row2", 10));
    verifyRewardResults(resultList, rewards, "rewards");

    result = retryQueryExecutionTillFinished(TEST_NAMESPACE, "select * from dataset_userRewards", 5);
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    rewards = new HashSet<>();
    rewards.add(new Rewards("row1", null, "samuel", 10));
    rewards.add(new Rewards("row2", null, "jackson", 10));
    verifyRewardResults(resultList, rewards, "user");

    result = retryQueryExecutionTillFinished(TEST_NAMESPACE, "select * from dataset_itemRewards", 5);
    resultList = Lists.newArrayList(result);
    Assert.assertEquals(2, resultList.size());
    rewards = new HashSet<>();
    rewards.add(new Rewards("row1", "scotch", null, 5));
    rewards.add(new Rewards("row2", "island", null, 5));
    verifyRewardResults(resultList, rewards, "item");
  }

  // have noticed errors with hivemetastore for table not found,
  // which didn't appear on consecutive run, so adding retry logic for query execute
  private ExploreExecutionResult retryQueryExecutionTillFinished(NamespaceId namespace, String query, int retryCount)
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
      return Objects.hash(rowKey, userId, item, rewards);
    }

    public String toString() {
      return String.format("Rowkey : %s, userId : %s, item : %s, rewards : %s ", rowKey, userId, item, rewards);
    }
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
