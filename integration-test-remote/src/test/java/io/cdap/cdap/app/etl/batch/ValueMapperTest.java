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

package io.cdap.cdap.app.etl.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration Test for ValueMapper.
 */
public class ValueMapperTest extends ETLTestBase {

  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String SALARY = "salary";
  private static final String DESIGNATION_ID = "designationid";
  private static final String DESIGNATION_NAME = "designationName";

  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord",
                    Schema.Field.of(ValueMapperTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.DESIGNATION_ID,
                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))));


  private static final Schema SINK_SCHEMA =
    Schema.recordOf("sinkRecord",
                    Schema.Field.of(ValueMapperTest.ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.NAME, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.SALARY, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ValueMapperTest.DESIGNATION_NAME, Schema.of(Schema.Type.STRING)));


  @Test
  public void test() throws Exception {

    String inputTable = "employeeTable";

    ETLStage source =
      new ETLStage("TableSource", new ETLPlugin("Table",
                                              BatchSource.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, inputTable,
                                                Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> sourceproperties = new ImmutableMap.Builder<String, String>()
      .put("mapping", "designationid:designationLookupTable:designationName")
      .put("defaults", "designationid:DEFAULTID")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("ValueMapper", Transform.PLUGIN_TYPE, sourceproperties, null));

    String sinkTable = "outputTable";

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table",
                                              BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, sinkTable,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "name",
                                                Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("ValueMapperTest");
    ApplicationManager appManager = deployApplication(appId, request);

    DataSetManager<KeyValueTable> dataSetManager = getKVTableDataset("designationLookupTable");
    KeyValueTable keyValueTable = dataSetManager.get();
    keyValueTable.write("1", "SE");
    keyValueTable.write("2", "SSE");
    keyValueTable.write("3", "ML");
    dataSetManager.flush();

    DataSetManager<Table> inputManager = getTableDataset(inputTable);
    Table empTable = inputManager.get();

    Put put = new Put(Bytes.toBytes(1)).add(ID, "100").add(NAME, "John").add(SALARY, "1000");
    empTable.put(put);
    Put put1 = new Put(Bytes.toBytes(2)).add(ID, "101").add(NAME, "Kerry").add(SALARY, "1030").add(DESIGNATION_ID, "2");
    empTable.put(put1);
    Put put2 = new Put(Bytes.toBytes(3)).add(ID, "102").add(NAME, "Mathew").add(SALARY, "1230").add(DESIGNATION_ID, "");
    empTable.put(put2);
    inputManager.flush();

    WorkflowManager mrManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    DataSetManager<Table> tableManager = getTableDataset(sinkTable);
    Table table = tableManager.get();
    Row row = table.get(Bytes.toBytes("John"));

    Assert.assertEquals("100", row.getString(ID));
    Assert.assertEquals("DEFAULTID", row.getString(DESIGNATION_NAME));

  }

}
