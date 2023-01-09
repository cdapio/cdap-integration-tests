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
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
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
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Properties;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> table2 = getKVTableDataset("table2");
    KeyValueTable outputTable = table2.get();
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
    }
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
}
