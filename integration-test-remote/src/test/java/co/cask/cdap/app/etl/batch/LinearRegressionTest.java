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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for LinearRegressionTrainer and LinearRegressionPredictor classes.
 */
@Category({
  // We don't support spark on these distros
  HDP20Incompatible.class,
  HDP21Incompatible.class,
  CDH51Incompatible.class,
  CDH52Incompatible.class,
  // this test is only compatible with spark v1.3.0 onwards and cdh5.3 uses spark v1.2.0
  CDH53Incompatible.class,
  // Currently, coopr doesn't provision MapR cluster with Spark
  MapR5Incompatible.class // MapR51 category is used for all MapR version
})
public class LinearRegressionTest extends ETLTestBase {
  private static final String SPARK_SINK = "linear-regression-spark-sink";
  private static final String SPARK_COMPUTE = "linear-regression-spark-compute";
  private static final String LABELED_RECORDS = "labeledRecords";

  private final Schema SCHEMA = Schema.recordOf("input-record", Schema.Field.of("age", Schema.of(Schema.Type.INT)),
                                                Schema.Field.of("height", Schema.of(Schema.Type.DOUBLE)),
                                                Schema.Field.of("smoke", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("gender", Schema.of(Schema.Type.STRING)));

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink(LinearRegressionTrainer) to train a model
    testSparkSink();
    // use a SparkCompute(LinearRegressionPredictor) to label all records going through the pipeline, using the model
    // build with the SparkSink
    testSparkCompute();
  }

  private void testSparkSink() throws Exception {

    ETLStage sourceStage = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, SPARK_SINK,
      Properties.Table.PROPERTY_SCHEMA, getTrainerSchema(SCHEMA).toString()), null));

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("fileSetName", "linear-regression-model")
      .put("path", "linearRegression")
      .put("featuresToInclude", "age")
      .put("labelField", "lung_capacity")
      .put("numIterations", "50")
      .put("stepSize", "0.001")
      .build();

    ETLStage sinkStage = new ETLStage("SparkSink", new ETLPlugin("LinearRegressionTrainer", SparkSink.PLUGIN_TYPE,
                                                                 properties, null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), sinkStage.getName())
      .build();

    // write data to be used for training the model
    DataSetManager<Table> inputManager = getTableDataset(SPARK_SINK);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 6, 62.1, "no", "male", 6.475);
    putValues(inputTable, 2, 18, 74.7, "yes", "female", 10.125);
    putValues(inputTable, 3, 16, 69.7, "no", "female", 9.550);
    putValues(inputTable, 4, 14, 71.0, "no", "male", 11.125);
    putValues(inputTable, 5, 5, 56.9, "no", "male", 4.800);
    inputManager.flush();

    ApplicationManager appManager = deployApplication(Id.Application.from(TEST_NAMESPACE, "Trainer"),
                                                      getBatchAppRequestV2(config));
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }

  private void testSparkCompute() throws Exception {

    ETLStage sourceStage = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, SPARK_COMPUTE,
      Properties.Table.PROPERTY_SCHEMA, SCHEMA.toString()), null));

    ETLStage computeStage = new ETLStage("SparkCompute",
                                         new ETLPlugin("LinearRegressionPredictor", SparkCompute.PLUGIN_TYPE,
                                                       ImmutableMap.of("fileSetName", "linear-regression-model",
                                                                       "path", "linearRegression",
                                                                       "featuresToInclude", "age",
                                                                       "predictionField", "lung_capacity"), null));

    ETLStage sinkStage = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, LABELED_RECORDS,
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "age",
      Properties.Table.PROPERTY_SCHEMA, getTrainerSchema(SCHEMA).toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(computeStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), computeStage.getName())
      .addConnection(computeStage.getName(), sinkStage.getName())
      .build();

    // write some data to be predicted
    DataSetManager<Table> inputManager = getTableDataset(SPARK_COMPUTE);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 11, 58.7, "no", "female", null);
    putValues(inputTable, 2, 8, 63.3, "no", "male", null);
    inputManager.flush();

    ApplicationManager appManager = deployApplication(Id.Application.from(TEST_NAMESPACE, "Predictor"),
                                                      getBatchAppRequestV2(config));
    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(LABELED_RECORDS);
    Table outputTable = outputManager.get();

    Row firstRow = outputTable.get(Bytes.toBytes(11));
    Assert.assertEquals(6.225, firstRow.getDouble("lung_capacity"), 0.5);

    Row seconRow = outputTable.get(Bytes.toBytes(8));
    Assert.assertEquals(4.950, seconRow.getDouble("lung_capacity"), 0.5);
  }

  private void putValues(Table inputTable, int index, int age, double height, String smoke, String gender,
                         Double lung_capacity) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("age", age);
    put.add("height", height);
    put.add("smoke", smoke);
    put.add("gender", gender);
    if (lung_capacity != null) {
      put.add("lung_capacity", lung_capacity);
    }
    inputTable.put(put);
  }

  private Schema getTrainerSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of("lung_capacity", Schema.of(Schema.Type.DOUBLE)));
    return Schema.recordOf(schema.getRecordName() + ".predicted", fields);
  }
}
