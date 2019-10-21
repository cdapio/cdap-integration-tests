/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests reading to and writing from Google BigQuery within a Dataproc cluster.
 */
public class GoogleBigQueryTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryTest.class);
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";

  private static final Field[] SIMPLE_FIELDS_SCHEMA = new Field[] {
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] UPDATED_FIELDS_SCHEMA = new Field[] {
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("numeric_value", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("timestamp_value", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] FULL_FIELDS_SCHEMA = new Field[] {
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("numeric_value", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("timestamp_value", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("bytes_value", LegacySQLTypeName.BYTES).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("time_value", LegacySQLTypeName.TIME).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("datetime_value", LegacySQLTypeName.DATETIME).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("string_array", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build(),
    Field.newBuilder("single_record", LegacySQLTypeName.RECORD,
                     Field.newBuilder("string_value", LegacySQLTypeName.STRING)
                       .setMode(Field.Mode.NULLABLE).build(),
                     Field.newBuilder("int_value", LegacySQLTypeName.INTEGER)
                       .setMode(Field.Mode.NULLABLE).build())
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("complex_record", LegacySQLTypeName.RECORD,
                     Field.newBuilder("simple_record", LegacySQLTypeName.RECORD,
                                      Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN)
                                        .setMode(Field.Mode.NULLABLE).build(),
                                      Field.newBuilder("record_string", LegacySQLTypeName.STRING)
                                        .setMode(Field.Mode.NULLABLE).build())
                       .setMode(Field.Mode.NULLABLE).build(),
                     Field.newBuilder("float_value", LegacySQLTypeName.FLOAT)
                       .setMode(Field.Mode.NULLABLE).build())
      .setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("record_array", LegacySQLTypeName.RECORD,
                     Field.newBuilder("r_a_string", LegacySQLTypeName.STRING)
                       .setMode(Field.Mode.NULLABLE).build(),
                     Field.newBuilder("r_a_int", LegacySQLTypeName.INTEGER)
                       .setMode(Field.Mode.NULLABLE).build())
      .setMode(Field.Mode.REPEATED).build()
  };

  private static final Field[] UPDATE_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] SIMPLE_UPDATE_FIELDS_SCHEMA = new Field[] {
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
  };

  private static String bigQueryDataset;
  private static Dataset dataset;
  private static BigQuery bq;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    UUID uuid = UUID.randomUUID();
    bigQueryDataset = "bq_dataset_" + uuid.toString().replaceAll("-", "_");
    try (InputStream inputStream = new ByteArrayInputStream(
      getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8))) {
      bq = BigQueryOptions.newBuilder()
        .setProjectId(getProjectId())
        .setCredentials(GoogleCredentials.fromStream(inputStream))
        .build()
        .getService();
    }
    createDataset();
  }

  @AfterClass
  public static void testClassClear() {
    deleteDatasets();
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!bigQueryPluginExists(dataPipelineId, BatchSource.PLUGIN_TYPE)) {
          return false;
        }
        return bigQueryPluginExists(dataPipelineId, BatchSink.PLUGIN_TYPE);
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {
  }

  /* Test check read data from exists table and store them in new table.
   * Input data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   * Starting sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value":0.1
   *  "boolean_value": true
   * Expected ending sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   */
  @Test
  public void testReadDataAndStoreInNewTable() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    Assert.assertFalse(exists(destinationTableName));

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInNewTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(exists(destinationTableName));
    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  /* Test check read data from exists table and store them in exist table.
   * Input data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value":0.1
   *  "boolean_value": true
   * Starting sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   * Expected ending sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   */
  @Test
  public void testReadDataAndStoreInExistingTable() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);

    Assert.assertTrue(exists(destinationTableName));

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInExistingTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  /* Test check read data from exists table and store them in exist table with updating table schema.
   * Input data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   * Destination table schema:
   *  "string_value", LegacySQLTypeName.STRING
   *  "int_value", LegacySQLTypeName.INTEGER
   *  "float_value", LegacySQLTypeName.FLOAT
   *  "boolean_value", LegacySQLTypeName.BOOLEAN
   * Starting sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   * Expected ending sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   * Expected destination table schema:
   *  "string_value", LegacySQLTypeName.STRING
   *  "int_value", LegacySQLTypeName.INTEGER
   *  "float_value", LegacySQLTypeName.FLOAT
   *  "boolean_value", LegacySQLTypeName.BOOLEAN
   *  "numeric_value", LegacySQLTypeName.NUMERIC
   *  "timestamp_value", LegacySQLTypeName.TIMESTAMP
   *  "date_value", LegacySQLTypeName.DATE
   */
  @Test
  public void testReadDataAndStoreWithUpdateTableSchema() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, UPDATED_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, Collections.singletonList(getUpdateSource()));

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);

    Schema sourceSchema = getUpdatedTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "true")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeWithUpdateTableSchema");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  /* Test check processing all BigQuery types.
   * Input data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   *  "time_value": "01:41:35.220000",
   *  "datetime_value": "2014-08-01 01:41:35.220000"
   *  "string_array": ["a_string_1", "a_string_2"]}
   *  "single_record": {"string_value":"string_1","int_value":10}
   *  "complex_record": {"simple_record":{"boolean_value":true,"record_string":"r_string"},"float_value":0.25}
   *  "record_array":[{"r_a_string":"r_a_string_1","r_a_int":100},{"r_a_string":"r_a_string_2","r_a_int":200}]}
   * Starting sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   *  "time_value": "01:41:35.220000",
   *  "datetime_value": "2014-08-01 01:41:35.220000"
   *  "string_array": ["a_string_1", "a_string_2"]}
   *  "single_record": {"string_value":"string_1","int_value":10}
   *  "complex_record": {"simple_record":{"boolean_value":true,"record_string":"r_string"},"float_value":0.25}
   *  "record_array":[{"r_a_string":"r_a_string_1","r_a_int":100},{"r_a_string":"r_a_string_2","r_a_int":200}]}
   * Expected ending sink data:
   *  "string_value": "string_1"
   *  "int_value": 1
   *  "float_value": 0.1
   *  "boolean_value": true
   *  "numeric_value": 123.456
   *  "timestamp_value": "2014-08-01 12:41:35.220000+00:00"
   *  "date_value": "2014-08-01"
   *  "bytes_value": "MTE1"
   *  "time_value": "01:41:35.220000",
   *  "datetime_value": "2014-08-01 01:41:35.220000"
   *  "string_array": ["a_string_1", "a_string_2"]}
   *  "single_record": {"string_value":"string_1","int_value":10}
   *  "complex_record": {"simple_record":{"boolean_value":true,"record_string":"r_string"},"float_value":0.25}
   *  "record_array":[{"r_a_string":"r_a_string_1","r_a_int":100},{"r_a_string":"r_a_string_2","r_a_int":200}]}
   */
  @Test
  public void testProcessingAllBigQuerySupportTypes() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, FULL_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, Collections.singletonList(getFullSource()));

    Schema sourceSchema = getFullTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-allBigQueryTypes");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(exists(destinationTableName));

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName, Collections.singleton("datetime_value"));
  }

  /* Test check the Update operation without updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":10,"float_value":1.1,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   */
  @Test
  public void testUpdateOperationWithoutSchemaUpdate() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationSourceString());

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPDATE")
      .put("relationTableKey", "string_value")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-updateWithoutSchemaUpdate");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_FIELDS_SCHEMA);
  }

  /* Test check the Upsert operation without updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":10,"float_value":1.1,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  @Test
  public void testUpsertOperationWithoutSchemaUpdate() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationSourceString());

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPSERT")
      .put("relationTableKey", "string_value")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-upsertWithoutSchemaUpdate");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")),
        SIMPLE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_FIELDS_SCHEMA);
  }

  /* Test check the Insert operation with updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"id":100,"string_value":"string_0","boolean_value":true}
   *  {"id":101,"string_value":"string_1","boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"id":100,"string_value":"string_0","int_value":null,"float_value":null,"boolean_value":true}
   *  {"id":101,"string_value":"string_1","int_value":null,"float_value":null,"boolean_value":false}
   *  {"id":null,"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"id":null,"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"id":null,"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  @Test
  public void testInsertOperationWithSchemaUpdate() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationSourceString());

    createTestTable(bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "true")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-insertWithSchemaUpdate");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "101")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)),
        SIMPLE_UPDATE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_UPDATE_FIELDS_SCHEMA);
  }

  /* Test check the Update operation with updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"id":100,"string_value":"string_0","boolean_value":true}
   *  {"id":101,"string_value":"string_1","boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"id":100,"string_value":"string_0","int_value":null,"float_value":null,"boolean_value":true}
   *  {"id":101,"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   */
  @Test
  public void testUpdateOperationWithSchemaUpdate() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationSourceString());

    createTestTable(bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPDATE")
      .put("relationTableKey", "string_value")
      .put("allowSchemaRelaxation", "true")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-updateWithSchemaUpdate");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "101")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_UPDATE_FIELDS_SCHEMA);
  }

  /* Test check the Upsert operation with updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"id":100,"string_value":"string_0","boolean_value":true}
   *  {"id":101,"string_value":"string_1","boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"id":100,"string_value":"string_0","int_value":null,"float_value":null,"boolean_value":true}
   *  {"id":101,"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"id":null,"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"id":null,"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  @Test
  public void testUpsertOperationWithSchemaUpdate() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationSourceString());

    createTestTable(bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPSERT")
      .put("relationTableKey", "string_value")
      .put("allowSchemaRelaxation", "true")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-upsertWithSchemaUpdate");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "101")),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)),
        SIMPLE_UPDATE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)),
        SIMPLE_UPDATE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_UPDATE_FIELDS_SCHEMA);
  }

  /* Test check the Update operation with dedupe source data.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_1","int_value":2,"float_value":0.2,"boolean_value":false}
   * Destination table:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":10,"float_value":1.1,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_1","int_value":2,"float_value":0.2,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   */
  @Test
  public void testUpdateOperationWithDedupeSourceData() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationUpdateSourceStringWithDupe());

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPDATE")
      .put("relationTableKey", "string_value")
      .put("dedupeBy", "int_value ASC")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 2;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-updateWithDedupeSourceData");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_FIELDS_SCHEMA);
  }

  /* Test check the Upsert operation without updating destination table schema.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_1","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":10,"float_value":1.1,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_1","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_0","int_value":0,"float_value":0,"boolean_value":true}
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  @Test
  public void testUpsertOperationWithDedupeSourceData() throws Exception {
    String testId = getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, sourceTableName, getOperationUpsertSourceStringWithDupe());

    createTestTable(bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    insertData(bigQueryDataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("operation", "UPSERT")
      .put("relationTableKey", "string_value")
      .put("dedupeBy", "float_value DESC")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-upsertWithDedupeSourceData");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.0"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")),
        SIMPLE_FIELDS_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")),
        SIMPLE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_FIELDS_SCHEMA);
  }

  private void assertSchemaEquals(String expectedTableName, String actualTableName) {
    TableId expectedTableId = TableId.of(bigQueryDataset, expectedTableName);
    TableId actualTableId = TableId.of(bigQueryDataset, actualTableName);

    com.google.cloud.bigquery.Schema expectedSchema = getTableSchema(expectedTableId);
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(actualTableId);

    Assert.assertEquals(expectedSchema.getFields().size(), actualSchema.getFields().size());

    FieldList expectedFieldList = expectedSchema.getFields();
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

  private void assertActualTable(String actualTableName, List<FieldValueList> expectedResult, Field[] schema) {
    TableId actualTableId = TableId.of(bigQueryDataset, actualTableName);
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(actualTableId);
    List<FieldValueList> actualResult = getResultTableData(actualTableId, actualSchema);

    Assert.assertEquals(String.format("Expected row count '%d', actual row count '%d'.", expectedResult.size(),
                                      actualResult.size()), expectedResult.size(), actualResult.size());

    actualResult.sort((o1, o2) ->
                      o1.get("string_value").toString().compareToIgnoreCase(o2.get("string_value").toString()));

    for (int i = 0; i < expectedResult.size(); i++) {
      FieldValueList expectedFieldValueList = expectedResult.get(i);
      FieldValueList actualFieldValueList = actualResult.get(i);
      Arrays.stream(schema).map(Field::getName)
        .forEach(fieldName -> {
          FieldValue expected = expectedFieldValueList.get(fieldName);
          FieldValue actual = actualFieldValueList.get(fieldName);
          String message = String.format("Values differ for field '%s'. Expected '%s' but was '%s'.", fieldName,
                                         expected, actual);
          Assert.assertEquals(message, expected, actual);
        });
    }
  }

  private void assertTableEquals(String expectedTableName, String actualTableName) {
    assertTableEquals(expectedTableName, actualTableName, Collections.emptySet());
  }

  private void assertTableEquals(String expectedTableName, String actualTableName, Set<String> datetimeFields) {
    TableId expectedTableId = TableId.of(bigQueryDataset, expectedTableName);
    TableId actualTableId = TableId.of(bigQueryDataset, actualTableName);

    com.google.cloud.bigquery.Schema expectedSchema = getTableSchema(expectedTableId);
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(actualTableId);

    List<FieldValueList> expectedResult = getResultTableData(expectedTableId, expectedSchema);
    List<FieldValueList> actualResult = getResultTableData(actualTableId, actualSchema);
    Assert.assertEquals(expectedResult.size(), actualResult.size());

    for (int i = 0; i < expectedResult.size(); i++) {
      FieldValueList expectedFieldValueList = expectedResult.get(i);
      FieldValueList actualFieldValueList = actualResult.get(i);
      Objects.requireNonNull(expectedSchema).getFields().stream()
        .map(Field::getName)
        .forEach(fieldName -> {
          FieldValue expected = expectedFieldValueList.get(fieldName);
          FieldValue actual = actualFieldValueList.get(fieldName);
          if (datetimeFields.contains(fieldName)) {
            // precision is lost in CDAP, which only goes to micros and not nanos
            String message = String.format("Values differ for field '%s'. Expected = '%s', actual = '%s'", fieldName,
                                           expected, actual);
            Assert.assertTrue(message, expected.getStringValue().startsWith(actual.getStringValue()));
          } else {
            String message = String.format("Values differ for field '%s'.", fieldName);
            Assert.assertEquals(message, expected, actual);
          }
        });
    }
  }

  private boolean bigQueryPluginExists(ArtifactId dataPipelineId, String pluginType) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM).stream()
      .anyMatch(pluginSummary -> BIG_QUERY_PLUGIN_NAME.equals(pluginSummary.getName()));
  }

  private GoogleBigQueryTest.DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                                                 Map<String, String> sinkProperties,
                                                                 String applicationName) throws Exception {

    ETLStage source = new ETLStage("BigQuerySourceStage",
                                   new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties,
                                                 GOOGLE_CLOUD_ARTIFACT));
    ETLStage sink = new ETLStage("BigQuerySinkStage",
                                 new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties,
                                               GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private static void createDataset() {
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDataset).build();
    LOG.info("Creating dataset {}", bigQueryDataset);
    dataset = bq.create(datasetInfo);
    LOG.info("Created dataset {}", bigQueryDataset);
  }

  private com.google.cloud.bigquery.Schema getTableSchema(TableId tableId) {
    return bq.getTable(tableId).getDefinition().getSchema();
  }

  private static List<FieldValueList> getResultTableData(TableId tableId, com.google.cloud.bigquery.Schema schema) {
    TableResult tableResult = bq.listTableData(tableId, schema);
    List<FieldValueList> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::add);
    return result;
  }

  private static void deleteDatasets() {
    DatasetId datasetId = dataset.getDatasetId();
    LOG.info("Deleting dataset {}", bigQueryDataset);
    boolean deleted = bq.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    if (deleted) {
      LOG.info("Deleted dataset {}", bigQueryDataset);
    }
  }

  private boolean exists(String tableId) {
    return dataset.get(tableId) != null;
  }

  private void createTestTable(String datasetId, String tableId, Field[] fieldsSchema) {
    TableId table = TableId.of(datasetId, tableId);

    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fieldsSchema);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo.newBuilder(table, tableDefinition).build();
    TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();

    bq.create(tableInfo);
  }

  private void insertData(String datasetId, String tableId, Collection<JsonObject> source)
    throws IOException, InterruptedException {
    TableId table = TableId.of(datasetId, tableId);

    WriteChannelConfiguration writeChannelConfiguration =
      WriteChannelConfiguration.newBuilder(table).setFormatOptions(FormatOptions.json()).build();

    JobId jobId = JobId.newBuilder().setLocation(dataset.getLocation()).build();
    TableDataWriteChannel writer = bq.writer(jobId, writeChannelConfiguration);

    String sourceString = source.stream().map(JsonElement::toString).collect(Collectors.joining("\n"));
    try (OutputStream outputStream = Channels.newOutputStream(writer);
         InputStream inputStream = new ByteArrayInputStream(sourceString.getBytes(Charset.forName("UTF-8")))) {
      ByteStreams.copy(inputStream, outputStream);
    }

    Job job = writer.getJob();
    job.waitFor();
  }

  private String getUUID() {
    return UUID.randomUUID().toString().replaceAll("-", "_");
  }

  private static JsonObject getSimpleSource() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("boolean_value", true);
    return json;
  }

  private static JsonObject getUpdateSource() {
    JsonObject json = getSimpleSource();
    json.addProperty("numeric_value", 123.456);
    json.addProperty("timestamp_value", "2014-08-01 12:41:35.220000+00:00");
    json.addProperty("date_value", "2014-08-01");
    return json;
  }

  private static JsonObject getFullSource() {
    JsonObject json = getUpdateSource();
    json.addProperty("bytes_value", "MTE1");
    json.addProperty("time_value", "01:41:35.220000");
    json.addProperty("datetime_value", "2014-08-01 01:41:35.220000");
    JsonArray array = new JsonArray();
    for (String item : Arrays.asList("a_string_1", "a_string_2")) {
      array.add(new JsonPrimitive(item));
    }
    json.add("string_array", array);
    JsonObject record = new JsonObject();
    record.addProperty("string_value", "string_1");
    record.addProperty("int_value", 10);
    json.add("single_record", record);
    record = new JsonObject();
    JsonObject simpleRecord = new JsonObject();
    simpleRecord.addProperty("boolean_value", true);
    simpleRecord.addProperty("record_string", "r_string");
    record.add("simple_record", simpleRecord);
    record.addProperty("float_value", 0.25);
    json.add("complex_record", record);
    array = new JsonArray();
    record = new JsonObject();
    record.addProperty("r_a_string", "r_a_string_1");
    record.addProperty("r_a_int", 100);
    array.add(record);
    record = new JsonObject();
    record.addProperty("r_a_string", "r_a_string_2");
    record.addProperty("r_a_int", 200);
    array.add(record);
    json.add("record_array", array);
    return json;
  }

  private List<JsonObject> getOperationSourceString() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("boolean_value", true);
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_2");
    json.addProperty("int_value", 2);
    json.addProperty("float_value", 0.2);
    json.addProperty("boolean_value", false);
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_3");
    json.addProperty("int_value", 3);
    json.addProperty("float_value", 0.3);
    json.addProperty("boolean_value", false);
    list.add(json);

    return list;
  }

  private List<JsonObject> getOperationDestinationWithoutUpdateString() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_0");
    json.addProperty("int_value", 0);
    json.addProperty("float_value", 0);
    json.addProperty("boolean_value", true);
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 10);
    json.addProperty("float_value", 1.1);
    json.addProperty("boolean_value", false);
    list.add(json);

    return list;
  }

  private List<JsonObject> getOperationDestinationWithUpdateString() {
    JsonObject json = new JsonObject();
    json.addProperty("id", 100);
    json.addProperty("string_value", "string_0");
    json.addProperty("boolean_value", true);
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("id", 101);
    json.addProperty("string_value", "string_1");
    json.addProperty("boolean_value", false);
    list.add(json);

    return list;
  }

  private List<JsonObject> getOperationUpdateSourceStringWithDupe() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("boolean_value", true);
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 2);
    json.addProperty("float_value", 0.2);
    json.addProperty("boolean_value", false);
    list.add(json);

    return list;
  }

  private List<JsonObject> getOperationUpsertSourceStringWithDupe() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("boolean_value", true);
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 2);
    json.addProperty("float_value", 0.2);
    json.addProperty("boolean_value", false);
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_3");
    json.addProperty("int_value", 3);
    json.addProperty("float_value", 0.3);
    json.addProperty("boolean_value", false);
    list.add(json);

    return list;
  }

  private Schema getSimpleTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
      );
  }

  private Schema getUpdatedTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                Schema.Field.of("numeric_value", Schema.nullableOf(Schema.decimalOf(38, 9))),
                Schema.Field.of("timestamp_value", Schema.nullableOf(Schema.of(
                  Schema.LogicalType.TIMESTAMP_MICROS))),
                Schema.Field.of("date_value", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)))
      );
  }

  private Schema getFullTableSchema() {
    return Schema
      .recordOf("bigQuerySourceSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("numeric_value", Schema.nullableOf(Schema.decimalOf(38, 9))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                Schema.Field.of("timestamp_value", Schema.nullableOf(Schema.of(
                  Schema.LogicalType.TIMESTAMP_MICROS))),
                Schema.Field.of("date_value", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                Schema.Field.of("bytes_value", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                Schema.Field.of("time_value", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                Schema.Field.of("datetime_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("string_array", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("single_record", Schema.nullableOf(
                  Schema.recordOf("single_record",
                                  Schema.Field.of("string_value", Schema.nullableOf(
                                    Schema.of(Schema.Type.STRING))),
                                  Schema.Field.of("int_value", Schema.nullableOf(
                                    Schema.of(Schema.Type.LONG)))
                  ))
                ),
                Schema.Field.of("complex_record", Schema.nullableOf(
                  Schema.recordOf("complex_record", Schema.Field.of("simple_record", Schema.nullableOf(
                    Schema.recordOf("simple_record",
                                    Schema.Field.of("boolean_value", Schema.nullableOf(
                                      Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("record_string", Schema.nullableOf(
                                      Schema.of(Schema.Type.STRING))))
                                  )),
                                  Schema.Field.of("float_value", Schema.nullableOf(
                                    Schema.of(Schema.Type.DOUBLE))))
                                )
                ),
                Schema.Field.of("record_array",
                                Schema.arrayOf(
                                  Schema.recordOf("record_array",
                                                  Schema.Field.of("r_a_string", Schema.nullableOf(
                                                    Schema.of(Schema.Type.STRING))),
                                                  Schema.Field.of("r_a_int", Schema.nullableOf(
                                                    Schema.of(Schema.Type.LONG)))
                                  )
                                )
                )
      );
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }

}
