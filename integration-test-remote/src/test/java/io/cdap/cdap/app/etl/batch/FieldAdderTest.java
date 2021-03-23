/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import io.cdap.cdap.app.etl.gcp.GoogleBigQueryUtils;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.Transform;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class FieldAdderTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocETLTestBase.class);

  private static final String MULTI_FIELD_ADDER_PLUGIN_NAME = "MultiFieldAdder";
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";

  private static final String NON_MACRO_FIELD_VALUE = "key1:value1,key2:value2";
  private static final String MACRO_FIELD_VALUE = "key1:${value1},${key2}:value2";
  private static final String WHOLE_MACRO_FIELD_VALUE = "${fieldValue}";
  private static final Schema INPUT_SCHEMA = Schema.recordOf("record",
                                                             Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("surname", Schema.of(Schema.Type.STRING))
  );

  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("output",
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("surname", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("key1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("key2", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  private static final Field[] INPUT_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("id", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("name", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("surname", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
  };

  private static final Field[] EXPECTED_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("id", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("name", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("surname", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("key1", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("key2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
  };

  private static final com.google.cloud.bigquery.Schema EXPECTED_BIG_QUERY_SCHEMA =
    com.google.cloud.bigquery.Schema.of(EXPECTED_FIELDS_SCHEMA);
  private static BigQuery bq;
  private static String bigQueryDataset;
  private static Dataset dataset;

  @AfterClass
  public static void testClassClear() {
    deleteDatasets();
  }

  private static void createDataset() throws Exception {
    UUID uuid = UUID.randomUUID();
    bigQueryDataset = "bq_dataset_" + uuid.toString().replaceAll("-", "_");
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
    LOG.info("Creating dataset {}", bigQueryDataset);
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDataset).build();
    dataset = bq.create(datasetInfo);
    LOG.info("Created dataset {}", bigQueryDataset);
  }

  private static void deleteDatasets() {
    LOG.info("Deleting dataset {}", bigQueryDataset);
    boolean deleted = bq.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
    if (deleted) {
      LOG.info("Deleted dataset {}", bigQueryDataset);
    }
  }

  private static JsonObject getSimpleSource() {
    JsonObject json = new JsonObject();
    json.addProperty("id", "test_id");
    json.addProperty("name", "test_name");
    json.addProperty("surname", "test_surname");
    return json;
  }

  private static List<FieldValueList> getExpectedValues() {
    return ImmutableList.of(FieldValueList.of(
      Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test_id"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test_name"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test_surname"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value1"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value2")),
      EXPECTED_FIELDS_SCHEMA));
  }

  public static List<FieldValueList> getResultTableData(BigQuery bq, TableId tableId,
                                                        com.google.cloud.bigquery.Schema schema) {
    TableResult tableResult = bq.listTableData(tableId, schema);
    List<FieldValueList> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::add);
    return result;
  }

  @Before
  public void testSetup() throws Exception {
    createDataset();
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSource.PLUGIN_TYPE,
                                BIG_QUERY_PLUGIN_NAME)) {
          return false;
        }
        return GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSink.PLUGIN_TYPE,
                                BIG_QUERY_PLUGIN_NAME);
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {

  }

  @Test
  public void testMultiFieldAdderNonMacroValues() throws Exception {
    testMultiFieldAdderNonMacroValues(Engine.SPARK);
    testMultiFieldAdderNonMacroValues(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderNonMacroValues(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();
    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, INPUT_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());

    multiFieldAdderConfig(MULTI_FIELD_ADDER_PLUGIN_NAME + engine + "-nonMacroValues", sourceTableName,
                          destinationTableName, engine, NON_MACRO_FIELD_VALUE, args);

    Assert.assertNotNull(dataset.get(destinationTableName));
    assertTableData(destinationTableName);
    assertSchemaEquals(destinationTableName);
  }

  @Test
  public void testMultiFieldAdderMacroValues() throws Exception {
    testMultiFieldAdderMacroValues(Engine.SPARK);
    testMultiFieldAdderMacroValues(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderMacroValues(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();
    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, INPUT_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());
    args.put("value1", "value1");
    args.put("key2", "key2");

    multiFieldAdderConfig(MULTI_FIELD_ADDER_PLUGIN_NAME + engine + "-macroValues", sourceTableName,
                          destinationTableName, engine, MACRO_FIELD_VALUE, args);

    Assert.assertNotNull(dataset.get(destinationTableName));
    assertTableData(destinationTableName);
    assertSchemaEquals(destinationTableName);
  }

  @Test
  public void testMultiFieldAdderWholeMacroValue() throws Exception {
    testMultiFieldAdderWholeMacroValue(Engine.SPARK);
    testMultiFieldAdderWholeMacroValue(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderWholeMacroValue(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();
    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, INPUT_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());
    args.put("fieldValue", NON_MACRO_FIELD_VALUE);

    multiFieldAdderConfig(MULTI_FIELD_ADDER_PLUGIN_NAME + engine + "-wholeMacroValues", sourceTableName,
                          destinationTableName, engine, WHOLE_MACRO_FIELD_VALUE, args);

    Assert.assertNotNull(dataset.get(destinationTableName));
    assertTableData(destinationTableName);
    assertSchemaEquals(destinationTableName);
  }

  private void multiFieldAdderConfig(String applicationName, String sourceTableName, String destinationTableName,
                                     Engine engine, String fieldValue, Map<String, String> args) throws Exception {
    installPluginFromHub("hydrator-plugin-add-field-transform", "field-adder", "2.1.1");

    ETLStage source = getSourceStage(sourceTableName);
    ETLStage transform = getTransformStage(fieldValue);
    ETLStage sink = getBigQuerySink(destinationTableName);
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(pipelineConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    startWorkFlow(appManager, ProgramRunStatus.COMPLETED, args);
  }

  private ETLStage getSourceStage(String sourceTableName) {
    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", INPUT_SCHEMA.toString())
      .build();
    return new ETLStage("BigQuerySourceStage",
                        new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProps,
                                      GOOGLE_CLOUD_ARTIFACT));

  }

  private ETLStage getTransformStage(String fieldValue) {
    return new ETLStage("transform",
                        new ETLPlugin(
                          MULTI_FIELD_ADDER_PLUGIN_NAME,
                          Transform.PLUGIN_TYPE,
                          ImmutableMap.of(
                            "fieldValue", fieldValue,
                            "inputSchema", INPUT_SCHEMA.toString()),
                          null));
  }

  private ETLStage getBigQuerySink(String tableName) {
    Map<String, String> props = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", tableName)
      .put("operation", "INSERT")
      .put("schema", "${schema}")
      .put("allowSchemaRelaxation", "false")
      .build();

    return new ETLStage("BigQuerySinkStage",
                        new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, props, GOOGLE_CLOUD_ARTIFACT));
  }

  private void assertSchemaEquals(String tableName) {
    TableId tableId = TableId.of(bigQueryDataset, tableName);
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(tableId).getDefinition().getSchema();

    assert actualSchema != null;
    Assert.assertEquals(EXPECTED_BIG_QUERY_SCHEMA.getFields().size(), actualSchema.getFields().size());

    FieldList expectedFieldList = EXPECTED_BIG_QUERY_SCHEMA.getFields();
    FieldList actualFieldList = actualSchema.getFields();
    IntStream.range(0, expectedFieldList.size()).forEach(i -> {
      Field expected = expectedFieldList.get(i);
      Field actual = actualFieldList.get(i);
      String fieldName = expected.getName();
      String message = String.format("Values differ for field '%s'.", fieldName);
      if (expected.getType() == LegacySQLTypeName.DATETIME) {
        Assert.assertEquals(message, actual.getType(), LegacySQLTypeName.STRING);
      } else {
        Assert.assertEquals(message, expected, actual);
      }
    });
  }

  public void assertTableData(String tableName) {
    List<FieldValueList> expectedResult = getExpectedValues();
    TableId actualTableId = TableId.of(bigQueryDataset, tableName);
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(actualTableId).getDefinition().getSchema();
    List<FieldValueList> actualResult = getResultTableData(bq, actualTableId, actualSchema);
    Assert.assertEquals(String.format("Expected row count '%d', actual row count '%d'.", expectedResult.size(),
                                      actualResult.size()), expectedResult.size(), actualResult.size());

    for (int i = 0; i < expectedResult.size(); i++) {
      FieldValueList expectedFieldValueList = expectedResult.get(i);
      FieldValueList actualFieldValueList = actualResult.get(i);
      Arrays.stream(EXPECTED_FIELDS_SCHEMA).map(Field::getName)
        .forEach(fieldName -> {
          FieldValue expected = expectedFieldValueList.get(fieldName);
          FieldValue actual = actualFieldValueList.get(fieldName);
          String message = String.format("Values differ for field '%s'. Expected '%s' but was '%s'.",
                                         fieldName, expected, actual);
          Assert.assertEquals(message, expected, actual);
        });
    }
  }
}
