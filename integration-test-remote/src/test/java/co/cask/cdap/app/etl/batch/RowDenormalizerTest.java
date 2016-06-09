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
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RowDenormalizerTest extends ETLTestBase {
  public static final String DENORMALIZER_SOURCE = "denormalizerSource";
  public static final String DENORMALIZER_SINK = "denormalizerSink";

  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("attribute", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("value", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "denormalizedRecord",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Lastname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Address", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );
  private static final Map<String, String> CONFIG_MAP = new ImmutableMap.Builder<String, String>()
    .put("keyField", "id")
    .put("nameField", "attribute")
    .put("valueField", "value")
    .put("outputFields", "Firstname,Lastname,Address")
    .build();

  @Test
  public void test() throws Exception {

    ETLStage sourceStage = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, DENORMALIZER_SOURCE,
      Properties.Table.PROPERTY_SCHEMA, INPUT_SCHEMA.toString()), null));

    ETLStage sinkStage = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, DENORMALIZER_SINK,
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "id",
      Properties.Table.PROPERTY_SCHEMA, OUTPUT_SCHEMA.toString()), null));

    ETLStage aggregateStage = new ETLStage("KeyAggregate", new ETLPlugin("RowDenormalizer",
                                                                         BatchAggregator.PLUGIN_TYPE,
                                                                         CONFIG_MAP, null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(aggregateStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), aggregateStage.getName())
      .addConnection(aggregateStage.getName(), sinkStage.getName())
      .build();

    ingestInputData(DENORMALIZER_SOURCE);

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "denormalizer-test");
    ApplicationManager appManager = deployApplication(appId, request);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(DENORMALIZER_SINK);
    Table outputTable = outputManager.get();

    Row rowJR = outputTable.get(Bytes.toBytes("JR"));
    Assert.assertEquals("John", rowJR.getString("Firstname"));
    Assert.assertEquals("Ray", rowJR.getString("Lastname"));
    Assert.assertEquals("NE Lakeside.", rowJR.getString("Address"));

    Row rowBL = outputTable.get(Bytes.toBytes("BL"));
    Assert.assertEquals("Brett", rowBL.getString("Firstname"));
    Assert.assertEquals("Lee", rowBL.getString("Lastname"));
    Assert.assertEquals("SE Lakeside.", rowBL.getString("Address"));
  }

  private void ingestInputData(String inputDatasetName) throws Exception {
    DataSetManager<Table> inputManager = getTableDataset(inputDatasetName);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1234567890000L, "JR", "Firstname", "John");
    putValues(inputTable, 2, 1234567890001L, "JR", "Lastname", "Ray");
    putValues(inputTable, 3, 1234567890001L, "JR", "Address", "NE Lakeside.");
    putValues(inputTable, 4, 1234567890002L, "BL", "Firstname", "Brett");
    putValues(inputTable, 5, 1234567890003L, "BL", "Lastname", "Lee");
    putValues(inputTable, 6, 1234567890004L, "BL", "Address", "SE Lakeside.");
    inputManager.flush();
  }

  private void putValues(Table inputTable, int index, long timestamp, String keyfield, String nameField,
                         String valueField) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("ts", timestamp);
    put.add("id", keyfield);
    put.add("attribute", nameField);
    put.add("value", valueField);
    inputTable.put(put);
  }
}
