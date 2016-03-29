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
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.batch.sink.BatchCubeSink;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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

    ETLStage source = new ETLStage("tableSource", new Plugin("Table",
                                                             ImmutableMap.of(
                                                               Properties.BatchReadableWritable.NAME, "inputTable",
                                                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                                               Properties.Table.PROPERTY_SCHEMA, schema.toString())));

    String aggregationGroup = "byUser:user";
    String measurement = "count:COUNTER";
    ETLStage sink =
      new ETLStage("CubeSink", new Plugin("Cube",
                                          ImmutableMap.of(Properties.Cube.DATASET_NAME, "batch_cube",
                                                          Properties.Cube.AGGREGATIONS, aggregationGroup,
                                                          Properties.Cube.MEASUREMENTS, measurement)));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testCubeAdapter");
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

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

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
