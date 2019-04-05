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

package io.cdap.cdap.app.etl.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
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
import io.cdap.plugin.batch.sink.BatchCubeSink;
import io.cdap.plugin.common.Properties;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link BatchCubeSink}.
 */
public class BatchCubeSinkTest extends ETLTestBase {
  @Test
  public void test() throws Exception {

    Schema schema = Schema.recordOf(
      "action",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    ETLStage source = new ETLStage("tableSource",
                                   new ETLPlugin("Table",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   Properties.BatchReadableWritable.NAME, "inputTable",
                                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                                   Properties.Table.PROPERTY_SCHEMA, schema.toString()),
                                                 null));

    String aggregationGroup = "byUser:user";
    String measurement = "count:COUNTER";
    ETLStage sink =
      new ETLStage("CubeSink", new ETLPlugin("Cube",
                                             BatchSink.PLUGIN_TYPE,
                                             ImmutableMap.of(Properties.Cube.DATASET_NAME, "batch_cube",
                                                             Properties.Cube.AGGREGATIONS, aggregationGroup,
                                                             Properties.Cube.MEASUREMENTS, measurement),
                                             null));

    ETLBatchConfig batchConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(batchConfig);
    ApplicationId appId = TEST_NAMESPACE.app("testCubeAdapter");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getTableDataset("inputTable");
    Table inputTable = inputManager.get();
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();

    long startTs = System.currentTimeMillis() / 1000;

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    long endTs = System.currentTimeMillis() / 1000;

    // verify
    DataSetManager<Cube> tableManager = getCubeDataset("batch_cube");
    Cube cube = tableManager.get();
    Collection<TimeSeries> result = cube.query(CubeQuery.builder()
                                                 .select().measurement("count", AggregationFunction.LATEST)
                                                 .from("byUser").resolution(1, TimeUnit.SECONDS)
                                                 .where().timeRange(startTs, endTs).limit(100).build());
    Assert.assertFalse(result.isEmpty());
    Iterator<TimeSeries> iterator = result.iterator();
    Assert.assertTrue(iterator.hasNext());
    TimeSeries timeSeries = iterator.next();
    Assert.assertEquals("count", timeSeries.getMeasureName());
    Assert.assertFalse(timeSeries.getTimeValues().isEmpty());
    Assert.assertEquals(5, timeSeries.getTimeValues().get(0).getValue());
    Assert.assertFalse(iterator.hasNext());
  }
}
