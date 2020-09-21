/*
 * Copyright © 2020 Cask Data, Inc.
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
package io.cdap.cdap.app.etl.gcp;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.Tasks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Test class for BigQueryMultiSink
 */
public class GoogleBigQueryMultiSinkTest extends DataprocETLTestBase {

  public static final String MULTISINK_RUNTIME_ARG = "multisink.%s";
  public static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  public static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";
  private static String bigQueryDatasetName;
  private static Dataset dataset;
  private static BigQuery bq;
  private static final Field[] SIMPLE_FIELDS_SRC_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("tablename", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE).build(),
  };
  private static final Field[] SIMPLE_FIELDS_DEST_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN)
      .setMode(Field.Mode.NULLABLE).build()
  };
  private static final Field[] SIMPLE_FIELDS_DEST_SCHEMA_MODIFIED = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT)
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN)
      .setMode(Field.Mode.NULLABLE).build()
  };
  public static final Schema OUTPUT_SCHEMA = Schema
    .recordOf("output.schema",
              Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
              Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
              Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
              Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
    );

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        return GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSource.PLUGIN_TYPE,
                                "BigQueryTable");
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {

  }

  @BeforeClass
  public static void testClassSetup() throws IOException {
    UUID uuid = UUID.randomUUID();
    bigQueryDatasetName = "bq_dataset_" + uuid.toString().replaceAll("-", "_");
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDatasetName).build();
    dataset = bq.create(datasetInfo);
  }

  @AfterClass
  public static void testClassClear() {
    bq.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
  }

  //Read from existing tables and store in two new tables
  @Test
  public void testStoreInTwoNewTables() throws Exception {
    String sourceTableName1 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String sourceTableName2 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName1 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName2 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();

    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName1, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName2, SIMPLE_FIELDS_SRC_SCHEMA);

    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName1,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName1)));
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName2,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName2)));

    Assert.assertFalse(dataset.get(destinationTableName1) != null);
    Assert.assertFalse(dataset.get(destinationTableName2) != null);

    Schema sourceSchema = getSimpleTableSchema();
    Map<String, String> srcProps1 = getSrcProps("bigQuery_source1", sourceSchema, sourceTableName1);
    Map<String, String> srcProps2 = getSrcProps("bigQuery_source2", sourceSchema, sourceTableName2);

    Map<String, String> destProps = getDestProps("bigQuery_multisink1");

    createAndStartPipeline(destinationTableName1, destinationTableName2, srcProps1, srcProps2,
                           destProps);

    //Destination tables should be created as part of the workflow.
    Assert.assertTrue(dataset.get(destinationTableName1) != null);
    Assert.assertTrue(dataset.get(destinationTableName2) != null);
    //verify that schema is same as the source tables minus the tablename field
    verifySchema(destinationTableName1,
                 new String[]{"string_value", "int_value", "float_value", "boolean_value"},
                 new String[]{"string", "integer", "float", "boolean"});
    verifySchema(destinationTableName2,
                 new String[]{"string_value", "int_value", "float_value", "boolean_value"},
                 new String[]{"string", "integer", "float", "boolean"});
    //verify that data is same as the source tables
    verifyData(sourceTableName1, destinationTableName1,
               Collections.singleton("tablename"));
    verifyData(sourceTableName2, destinationTableName2,
               Collections.singleton("tablename"));
  }

  //This test will currently fail due to - https://issues.cask.co/browse/PLUGIN-402 .
  // Uncomment and test this when it is fixed.
  //@Test
  public void testStoreInOneNewTable() throws Exception {
    String sourceTableName1 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName1 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();

    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName1, SIMPLE_FIELDS_SRC_SCHEMA);

    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName1,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName1)));

    Assert.assertFalse(dataset.get(destinationTableName1) != null);

    Schema sourceSchema = getSimpleTableSchema();
    Map<String, String> srcProps1 = getSrcProps("bigQuery_source1", sourceSchema, sourceTableName1);

    Map<String, String> destProps = getDestProps("bigQuery_multisink2");

    createAndStartSingleTablePipeline(destinationTableName1, srcProps1, destProps);

    Assert.assertTrue(dataset.get(destinationTableName1) != null);

    verifySchema(destinationTableName1,
                 new String[]{"string_value", "int_value", "float_value", "boolean_value"},
                 new String[]{"string", "integer", "float", "boolean"});
    verifyData(sourceTableName1, destinationTableName1, Collections.singleton("tablename"));
  }

  //Read from source tables and write into two existing tables
  @Test
  public void testStoreInTwoExistingTables() throws Exception {
    String sourceTableName1 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String sourceTableName2 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName1 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName2 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();

    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName1, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName2, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName1, SIMPLE_FIELDS_DEST_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName2, SIMPLE_FIELDS_DEST_SCHEMA);

    //insert data into both source and destination tables
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName1,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName1)));
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName2,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName2)));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName1,
                                   Collections.singletonList(getSimpleDestData()));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName2,
                                   Collections.singletonList(getSimpleDestData()));

    Schema sourceSchema = getSimpleTableSchema();
    Map<String, String> srcProps1 = getSrcProps("bigQuery_source1", sourceSchema, sourceTableName1);
    Map<String, String> srcProps2 = getSrcProps("bigQuery_source2", sourceSchema, sourceTableName2);

    Map<String, String> destProps = getDestProps("bigQuery_multisink_existing");

    createAndStartPipeline(destinationTableName1, destinationTableName2, srcProps1, srcProps2,
                           destProps);

    //Data should have been appended to the destination tables
    verifyDataIsAdded(sourceTableName1, destinationTableName1,
                      Collections.singleton("tablename"), 1);
    verifyDataIsAdded(sourceTableName2, destinationTableName2,
                      Collections.singleton("tablename"), 1);
  }

  //Read from source tables and write to destination tables with truncate flag set
  @Test
  public void testTruncateAndStoreInTwoExistingTables() throws Exception {
    String sourceTableName1 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String sourceTableName2 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName1 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName2 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();

    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName1, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName2, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName1, SIMPLE_FIELDS_DEST_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName2, SIMPLE_FIELDS_DEST_SCHEMA);

    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName1,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName1)));
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName2,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName2)));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName1,
                                   Collections.singletonList(getSimpleDestData()));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName2,
                                   Collections.singletonList(getSimpleDestData()));

    Schema sourceSchema = getSimpleTableSchema();
    Map<String, String> srcProps1 = getSrcProps("bigQuery_source1", sourceSchema, sourceTableName1);
    Map<String, String> srcProps2 = getSrcProps("bigQuery_source2", sourceSchema, sourceTableName2);
    //Truncate is set and update is not set
    Map<String, String> destProps = getDestProps("bigQuery_multisink_truncate", true, false);

    createAndStartPipeline(destinationTableName1, destinationTableName2, srcProps1, srcProps2,
                           destProps);

    //Data should match the source data tables
    verifyData(sourceTableName1, destinationTableName1, Collections.singleton("tablename"));
    verifyData(sourceTableName2, destinationTableName2, Collections.singleton("tablename"));
  }

  //Read from source and write to two destination tables with truncate and update schema flags set.
  @Test
  public void testUpdateAndStoreInTwoExistingTables() throws Exception {
    String sourceTableName1 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String sourceTableName2 =
      SOURCE_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName1 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();
    String destinationTableName2 =
      SINK_TABLE_NAME_TEMPLATE + GoogleBigQueryUtils.getUUID();

    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName1, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, sourceTableName2, SIMPLE_FIELDS_SRC_SCHEMA);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName1,
                       SIMPLE_FIELDS_DEST_SCHEMA_MODIFIED);
    GoogleBigQueryUtils
      .createTestTable(bq, bigQueryDatasetName, destinationTableName2,
                       SIMPLE_FIELDS_DEST_SCHEMA_MODIFIED);

    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName1,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName1)));
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName2,
                                   Collections.singletonList(getSimpleSourceData(destinationTableName2)));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName1,
                                   Collections.singletonList(getSimpleDestData()));
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName2,
                                   Collections.singletonList(getSimpleDestData()));

    Schema sourceSchema = getSimpleTableSchema();
    Map<String, String> srcProps1 = getSrcProps("bigQuery_source1", sourceSchema, sourceTableName1);
    Map<String, String> srcProps2 = getSrcProps("bigQuery_source2", sourceSchema, sourceTableName2);

    //Both truncate and relax schema set
    Map<String, String> destProps = getDestProps("bigQuery_multisink_truncate", true, true);

    createAndStartPipeline(destinationTableName1, destinationTableName2, srcProps1, srcProps2,
                           destProps);
    // Verify that the destination schema got updated to reflect source schema
    verifySchema(destinationTableName1,
                 new String[]{"string_value", "int_value", "float_value", "boolean_value"},
                 new String[]{"string", "integer", "float", "boolean"});
    verifySchema(destinationTableName2,
                 new String[]{"string_value", "int_value", "float_value", "boolean_value"},
                 new String[]{"string", "integer", "float", "boolean"});
    // Verify that the data matches the source tables
    verifyData(sourceTableName1, destinationTableName1, Collections.singleton("tablename"));
    verifyData(sourceTableName2, destinationTableName2, Collections.singleton("tablename"));
  }

  private void createAndStartPipeline(String destinationTableName1, String destinationTableName2,
                                      Map<String, String> srcProps1, Map<String, String> srcProps2,
                                      Map<String, String> destProps) throws Exception {
    ETLStage source1 = new ETLStage("BigQuerySourceStage1",
                                    new ETLPlugin("BigQueryTable", BatchSource.PLUGIN_TYPE, srcProps1,
                                                  GOOGLE_CLOUD_ARTIFACT));
    ETLStage source2 = new ETLStage("BigQuerySourceStage2",
                                    new ETLPlugin("BigQueryTable", BatchSource.PLUGIN_TYPE, srcProps2,
                                                  GOOGLE_CLOUD_ARTIFACT));
    ETLStage sink = new ETLStage("BigQueryMultiSinkStage",
                                 new ETLPlugin("BigQueryMultiTable", BatchSink.PLUGIN_TYPE, destProps,
                                               GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source1)
      .addStage(source2)
      .addStage(sink)
      .addConnection(source1.getName(), sink.getName())
      .addConnection(source2.getName(), sink.getName())
      .build();

    String multisink1 = String.format(MULTISINK_RUNTIME_ARG, destinationTableName1);
    String multisink2 = String.format(MULTISINK_RUNTIME_ARG, destinationTableName2);
    Map<String, String> args = new HashMap<>();
    args.put(multisink1, OUTPUT_SCHEMA.toString());
    args.put(multisink2, OUTPUT_SCHEMA.toString());
    deployAndStart(args, etlConfig);
  }

  private void deployAndStart(Map<String, String> props, ETLBatchConfig etlConfig)
    throws Exception {
    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE
      .app(String.format("BQMulti-Simple-%s", System.currentTimeMillis()));
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDatasetName);
    args.putAll(props);
    startWorkFlow(applicationManager, ProgramRunStatus.COMPLETED, args);
  }

  private void createAndStartSingleTablePipeline(String destinationTableName1,
                                                 Map<String, String> srcProps1, Map<String, String> destProps)
    throws Exception {
    ETLStage source1 = new ETLStage("BigQuerySourceStage1",
                                    new ETLPlugin("BigQueryTable", BatchSource.PLUGIN_TYPE, srcProps1,
                                                  GOOGLE_CLOUD_ARTIFACT));
    ETLStage sink = new ETLStage("BigQueryMultiSinkStage",
                                 new ETLPlugin("BigQueryMultiTable", BatchSink.PLUGIN_TYPE, destProps,
                                               GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source1)
      .addStage(sink)
      .addConnection(source1.getName(), sink.getName())
      .build();

    String multisink1 = String.format(MULTISINK_RUNTIME_ARG, destinationTableName1);
    Map<String, String> args = new HashMap<>();
    args.put(multisink1, OUTPUT_SCHEMA.toString());
    deployAndStart(args, etlConfig);
  }

  private void verifySchema(String tableName, String[] expectedFieldNames, String[] expectedTypes) {
    TableId tableId = TableId.of(bigQueryDatasetName, tableName);
    com.google.cloud.bigquery.Schema schema = bq.getTable(tableId).getDefinition().getSchema();

    Assert.assertEquals(schema.getFields().size(), expectedFieldNames.length);

    FieldList fieldsList = schema.getFields();
    IntStream.range(0, fieldsList.size()).forEach(i -> {
      Field field = fieldsList.get(i);
      String fieldName = field.getName();
      String fieldType = field.getType().toString();
      Assert.assertEquals(fieldName, expectedFieldNames[i]);
      Assert.assertEquals(fieldType.toLowerCase(), expectedTypes[i]);
    });
  }

  private void verifyData(String srcTable, String destTable, Set<String> filterFieldNames) {
    TableId destTableId = TableId.of(bigQueryDatasetName, destTable);
    com.google.cloud.bigquery.Schema destSchema = bq.getTable(destTableId).getDefinition().getSchema();
    List<FieldValueList> destTableData = GoogleBigQueryUtils
      .getResultTableData(bq, destTableId, destSchema);

    TableId srcTableId = TableId.of(bigQueryDatasetName, srcTable);
    com.google.cloud.bigquery.Schema srcSchema = bq.getTable(srcTableId).getDefinition().getSchema();
    List<FieldValueList> srcTableData = GoogleBigQueryUtils
      .getResultTableData(bq, srcTableId, srcSchema);
    //same number of rows
    Assert.assertEquals(destTableData.size(), srcTableData.size());
    //match all field values of all records
    for (int i = 0; i < srcTableData.size(); i++) {
      FieldValueList srcFieldValues = srcTableData.get(i);
      FieldValueList destFieldValues = destTableData.get(i);
      srcSchema.getFields().forEach(field -> {
        if (filterFieldNames.contains(field.getName())) {
          return;
        }
        Assert.assertTrue(
          srcFieldValues.get(field.getName()).equals(destFieldValues.get(field.getName())));
      });
    }
  }

  private void verifyDataIsAdded(String srcTable, String destTable, Set<String> filterFieldNames,
                                 int existingRecordCount)
    throws InterruptedException {
    TableId destTableId = TableId.of(bigQueryDatasetName, destTable);
    com.google.cloud.bigquery.Schema schema = bq.getTable(destTableId).getDefinition().getSchema();
    List<FieldValueList> destTableData = GoogleBigQueryUtils
      .getResultTableData(bq, destTableId, schema);

    TableId srcTableId = TableId.of(bigQueryDatasetName, srcTable);
    com.google.cloud.bigquery.Schema srcSchema = bq.getTable(srcTableId).getDefinition().getSchema();
    List<FieldValueList> srcTableData = GoogleBigQueryUtils
      .getResultTableData(bq, srcTableId, srcSchema);
    //Number of records =  existing + records from src tables
    Assert.assertEquals(destTableData.size(), srcTableData.size() + existingRecordCount);
    //All src values should be present in destination
    for (int i = 0; i < srcTableData.size(); i++) {
      FieldValueList srcFieldValues = srcTableData.get(i);
      Field srcField = srcSchema.getFields().get(0);
      List<FieldValueList> result = queryForRecord(destTable, srcFieldValues, srcField);
      Assert.assertTrue(result.size() == 1);
      FieldValueList destFieldValues = result.get(0);
      srcSchema.getFields().forEach(field -> {
        if (filterFieldNames.contains(field.getName())) {
          return;
        }
        Assert.assertTrue(
          srcFieldValues.get(field.getName()).equals(destFieldValues.get(field.getName())));
      });
    }
  }

  //Query for a specific record
  private List<FieldValueList> queryForRecord(String destTable, FieldValueList srcFieldValues,
                                              Field srcField) throws InterruptedException {
    String query = String
      .format("select * from %s.%s where %s='%s'", bigQueryDatasetName, destTable,
              srcField.getName(), srcFieldValues.get(srcField.getName()).getStringValue());
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    TableResult destTableRow = bq.query(queryConfig);
    Assert.assertTrue(destTableRow != null);
    List<FieldValueList> result = new ArrayList<>();
    destTableRow.iterateAll().forEach(result::add);
    return result;
  }

  private static JsonObject getSimpleSourceData(String tableName) {
    JsonObject json = getSimpleDestData();
    json.addProperty("tablename", tableName);
    return json;
  }

  private static JsonObject getSimpleDestData() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", GoogleBigQueryUtils.getUUID());
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("boolean_value", true);
    return json;
  }

  private Schema getSimpleTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                Schema.Field.of("tablename", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
  }

  private Map<String, String> getSrcProps(String referenceName, Schema sourceSchema,
                                          String srcTable) {
    return new ImmutableMap.Builder<String, String>()
      .put("referenceName", referenceName)
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", srcTable)
      .put("schema", sourceSchema.toString())
      .build();
  }

  private Map<String, String> getDestProps(String referenceName) {
    return getDestProps(referenceName, false, false);
  }

  private Map<String, String> getDestProps(String referenceName, boolean truncateTable,
                                           boolean relaxSchema) {
    return new ImmutableMap.Builder<String, String>()
      .put("referenceName", referenceName)
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("splitField", "tablename")
      .put("truncateTable", String.valueOf(truncateTable))
      .put("allowSchemaRelaxation", String.valueOf(relaxSchema))
      .build();
  }
}
