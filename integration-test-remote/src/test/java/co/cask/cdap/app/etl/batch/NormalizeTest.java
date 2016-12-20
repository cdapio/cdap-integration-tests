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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Integration Test for Normalize.
 */
public class NormalizeTest extends ETLTestBase {
  private static final String CUSTOMER_ID = "CustomerId";
  private static final String ITEM_ID = "ItemId";
  private static final String ITEM_COST = "ItemCost";
  private static final String PURCHASE_DATE = "PurchaseDate";
  private static final String ID = "Id";
  private static final String DATE = "Date";
  private static final String ATTRIBUTE_TYPE = "AttributeType";
  private static final String ATTRIBUTE_VALUE = "AttributeValue";
  private static final String CUSTOMER_ID_FIRST = "S23424242";
  private static final String CUSTOMER_ID_SECOND = "R45764646";
  private static final String ITEM_ID_ROW1 = "UR-AR-243123-ST";
  private static final String ITEM_ID_ROW2 = "SKU-234294242942";
  private static final String ITEM_ID_ROW3 = "SKU-567757543532";
  private static final String PURCHASE_DATE_ROW1 = "08/09/2015";
  private static final String PURCHASE_DATE_ROW2 = "10/12/2015";
  private static final String PURCHASE_DATE_ROW3 = "06/09/2014";
  private static final double ITEM_COST_ROW1 = 245.67;
  private static final double ITEM_COST_ROW2 = 67.90;
  private static final double ITEM_COST_ROW3 = 14.15;
  private static final Schema INPUT_SCHEMA =
    Schema.recordOf("inputSchema",
                    Schema.Field.of(CUSTOMER_ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ITEM_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of(ITEM_COST, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of(PURCHASE_DATE, Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("outputSchema",
                    Schema.Field.of(ID, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(DATE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ATTRIBUTE_TYPE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(ATTRIBUTE_VALUE, Schema.of(Schema.Type.STRING)));

  private static String validFieldMapping;
  private static String validFieldNormalizing;

  @BeforeClass
  public static void initializeData() {
    validFieldMapping = CUSTOMER_ID + ":" + ID + "," + PURCHASE_DATE + ":" + DATE;
    validFieldNormalizing = ITEM_ID + ":" + ATTRIBUTE_TYPE + ":" + ATTRIBUTE_VALUE + "," + ITEM_COST + ":"
      + ATTRIBUTE_TYPE + ":" + ATTRIBUTE_VALUE;
  }

  private ApplicationManager deployApplication(Map<String, String> sourceProperties, String applicationName,
                                               String inputDatasetName, String outputDatasetName) throws Exception {
    ETLStage source =
      new ETLStage("TableSource", new ETLPlugin("Table",
                                                BatchSource.PLUGIN_TYPE,
                                                ImmutableMap.of(
                                                  Properties.BatchReadableWritable.NAME, inputDatasetName,
                                                  Properties.Table.PROPERTY_SCHEMA, INPUT_SCHEMA.toString()), null));
    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("Normalize", Transform.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table",
                                              BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, ATTRIBUTE_VALUE,
                                                Properties.Table.PROPERTY_SCHEMA, OUTPUT_SCHEMA.toString()), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    return deployApplication(appId, appRequest);
  }

  private void startWorkFlow(ApplicationManager appManager) throws TimeoutException, InterruptedException {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void putData(int rowId, byte[] custId, byte[] itemId, byte[] itemCost, byte[] date, Table targetTable) {
    Put put = new Put(Bytes.toBytes(rowId)).add(CUSTOMER_ID, custId).add(ITEM_ID, itemId).add(ITEM_COST, itemCost)
      .add(PURCHASE_DATE, date);
    targetTable.put(put);
  }


  @Test
  public void testNormalize() throws Exception {
    String inputTable = "customerPurchaseTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("fieldMapping", validFieldMapping)
      .put("fieldNormalizing", validFieldNormalizing)
      .put("outputSchema", OUTPUT_SCHEMA.toString())
      .build();
    String outputTable = "customer360Table";

    ApplicationManager appManager = deployApplication(sourceProperties, "normalizeTest", inputTable, outputTable);

    DataSetManager<Table> inputManager = getTableDataset(inputTable);
    Table customerPurchaseTable = inputManager.get();

    putData(1, Bytes.toBytes(CUSTOMER_ID_FIRST), Bytes.toBytes(ITEM_ID_ROW1), Bytes.toBytes(ITEM_COST_ROW1),
            Bytes.toBytes(PURCHASE_DATE_ROW1), customerPurchaseTable);
    putData(2, Bytes.toBytes(CUSTOMER_ID_FIRST), Bytes.toBytes(ITEM_ID_ROW2), Bytes.toBytes(ITEM_COST_ROW2),
            Bytes.toBytes(PURCHASE_DATE_ROW2), customerPurchaseTable);
    putData(3, Bytes.toBytes(CUSTOMER_ID_SECOND), Bytes.toBytes(ITEM_ID_ROW3), Bytes.toBytes(ITEM_COST_ROW3),
            Bytes.toBytes(PURCHASE_DATE_ROW3), customerPurchaseTable);

    inputManager.flush();

    startWorkFlow(appManager);

    DataSetManager<Table> tableManager = getTableDataset(outputTable);
    Table table = tableManager.get();
    Row row = table.get(Bytes.toBytes(ITEM_ID_ROW1));
    verifyOutput(row, CUSTOMER_ID_FIRST, PURCHASE_DATE_ROW1);

    row = table.get(Bytes.toBytes(String.valueOf(ITEM_COST_ROW1)));
    verifyOutput(row, CUSTOMER_ID_FIRST, PURCHASE_DATE_ROW1);

    row = table.get(Bytes.toBytes(ITEM_ID_ROW2));
    verifyOutput(row, CUSTOMER_ID_FIRST, PURCHASE_DATE_ROW2);

    row = table.get(Bytes.toBytes(String.valueOf(ITEM_COST_ROW2)));
    verifyOutput(row, CUSTOMER_ID_FIRST, PURCHASE_DATE_ROW2);

    row = table.get(Bytes.toBytes(ITEM_ID_ROW3));
    verifyOutput(row, CUSTOMER_ID_SECOND, PURCHASE_DATE_ROW3);

    row = table.get(Bytes.toBytes(String.valueOf(ITEM_COST_ROW3)));
    verifyOutput(row, CUSTOMER_ID_SECOND, PURCHASE_DATE_ROW3);
  }

  private void verifyOutput(Row row, String id, String date) {
    Assert.assertEquals(id, row.getString(ID));
    Assert.assertEquals(date, row.getString(DATE));
  }

  @Test
  public void testNormalizeWithEmptyAttributeValue() throws Exception {
    String inputTable = "customerPurchaseWithEmptyTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put("fieldMapping", validFieldMapping)
      .put("fieldNormalizing", validFieldNormalizing)
      .put("outputSchema", OUTPUT_SCHEMA.toString())
      .build();
    String outputTable = "customer360WithEmptyTable";

    ApplicationManager appManager = deployApplication(sourceProperties, "normalizeWithEmptyTest", inputTable,
                                                      outputTable);

    DataSetManager<Table> inputManager = getTableDataset(inputTable);
    Table customerPurchaseTable = inputManager.get();

    putData(1, Bytes.toBytes(CUSTOMER_ID_FIRST), null, Bytes.toBytes(ITEM_COST_ROW1),
            Bytes.toBytes(PURCHASE_DATE_ROW1), customerPurchaseTable);
    putData(2, Bytes.toBytes(CUSTOMER_ID_FIRST), Bytes.toBytes(ITEM_ID_ROW2), null,
            Bytes.toBytes(PURCHASE_DATE_ROW2), customerPurchaseTable);
    putData(3, Bytes.toBytes(CUSTOMER_ID_SECOND), Bytes.toBytes(ITEM_ID_ROW3), Bytes.toBytes(ITEM_COST_ROW3),
            Bytes.toBytes(PURCHASE_DATE_ROW3), customerPurchaseTable);

    inputManager.flush();

    startWorkFlow(appManager);

    DataSetManager<Table> tableManager = getTableDataset(outputTable);
    Table table = tableManager.get();
    //Row for ItemId with null value must be empty
    Row row = table.get(Bytes.toBytes(ITEM_ID_ROW1));
    Assert.assertNull(row.getString(ID));
    Assert.assertNull(row.getString(DATE));
    Assert.assertNull(row.getString(ATTRIBUTE_TYPE));

    //Row for ItemCost with null value must be empty
    row = table.get(Bytes.toBytes(String.valueOf(ITEM_COST_ROW2)));
    Assert.assertNull(row.getString(ID));
    Assert.assertNull(row.getString(DATE));
    Assert.assertNull(row.getString(ATTRIBUTE_TYPE));
  }
}
