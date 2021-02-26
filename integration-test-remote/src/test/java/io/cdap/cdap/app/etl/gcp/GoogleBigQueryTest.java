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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.Engine;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Tests reading to and writing from Google BigQuery within a Dataproc cluster.
 */
public class GoogleBigQueryTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryTest.class);
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";

  private static final Field[] SIMPLE_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] TIME_PARTITION_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] UPDATED_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("numeric_value", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("timestamp_value", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build()
  };

  private static final Field[] FULL_FIELDS_SCHEMA = new Field[]{
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

  private static final Field[] SIMPLE_UPDATE_FIELDS_SCHEMA = new Field[]{
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
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
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
        if (!GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSource.PLUGIN_TYPE, BIG_QUERY_PLUGIN_NAME)) {
          return false;
        }
        return GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSink.PLUGIN_TYPE, BIG_QUERY_PLUGIN_NAME);
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {
  }

  @Test
  public void testReadDataAndStoreInNewTable() throws Exception {
    testReadDataAndStoreInNewTable(Engine.MAPREDUCE);
    testReadDataAndStoreInNewTable(Engine.SPARK);
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
  private void testReadDataAndStoreInNewTable(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    Assert.assertFalse(dataset.get(destinationTableName) != null);

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine + "-storeInNewTable", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "false");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(dataset.get(destinationTableName) != null);
    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testReadDataAndStoreInExistingTable() throws Exception {
    testReadDataAndStoreInExistingTable(Engine.MAPREDUCE);
    testReadDataAndStoreInExistingTable(Engine.SPARK);
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
  private void testReadDataAndStoreInExistingTable(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getSimpleSource()));

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);

    Assert.assertTrue(dataset.get(destinationTableName) != null);

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine + "-storeInExistingTable", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "false");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testReadDataAndStoreWithUpdateTableSchema() throws Exception {
    testReadDataAndStoreWithUpdateTableSchema(Engine.MAPREDUCE);
    testReadDataAndStoreWithUpdateTableSchema(Engine.SPARK);
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
  private void testReadDataAndStoreWithUpdateTableSchema(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, UPDATED_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getUpdateSource()));

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);

    Schema sourceSchema = getUpdatedTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME +
        engine + "-storeWithUpdateTableSchema", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "true");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testProcessingAllBigQuerySupportTypes() throws Exception {
    testProcessingAllBigQuerySupportTypes(Engine.MAPREDUCE);
    testProcessingAllBigQuerySupportTypes(Engine.SPARK);
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
  private void testProcessingAllBigQuerySupportTypes(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, FULL_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, Collections.singletonList(getFullSource()));

    Schema sourceSchema = getFullTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine + "-allBigQueryTypes", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "false");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(dataset.get(destinationTableName) != null);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName, Collections.singleton("datetime_value"));
  }

  @Test
  public void testUpdateOperationWithoutSchemaUpdate() throws Exception {
    testUpdateOperationWithoutSchemaUpdate(Engine.MAPREDUCE);
    testUpdateOperationWithoutSchemaUpdate(Engine.SPARK);
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
  private void testUpdateOperationWithoutSchemaUpdate(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("relationTableKey", "${key}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME +
        engine + "-updateWithoutSchemaUpdate", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "UPDATE");
    args.put("relax", "false");
    args.put("key", "string_value");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

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

  @Test
  public void testUpsertOperationWithoutSchemaUpdate() throws Exception {
    testUpsertOperationWithoutSchemaUpdate(Engine.MAPREDUCE);
    testUpsertOperationWithoutSchemaUpdate(Engine.SPARK);
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
  private void testUpsertOperationWithoutSchemaUpdate(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("relationTableKey", "${key}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-upsertWithoutSchemaUpdate", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "UPSERT");
    args.put("relax", "false");
    args.put("key", "string_value");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

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

  @Test
  public void testInsertOperationWithSchemaUpdate() throws Exception {
    testInsertOperationWithSchemaUpdate(Engine.MAPREDUCE);
    testInsertOperationWithSchemaUpdate(Engine.SPARK);
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
  private void testInsertOperationWithSchemaUpdate(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine + "-insertWithSchemaUpdate", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "true");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

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

  @Test
  public void testUpdateOperationWithSchemaUpdate() throws Exception {
    testUpdateOperationWithSchemaUpdate(Engine.MAPREDUCE);
    testUpdateOperationWithSchemaUpdate(Engine.SPARK);
  }

  @Test
  public void testInsertOperationWithIntegerPartition() throws Exception {
    testInsertOperationWithIntegerPartition(Engine.MAPREDUCE);
    testInsertOperationWithIntegerPartition(Engine.SPARK);
  }

  /* Test check the Insert operation with partition type Integer.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  private void testInsertOperationWithIntegerPartition(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = String.format("%s%s", SOURCE_TABLE_NAME_TEMPLATE, testId);
    String destinationTableName = String.format("%s%s", SINK_TABLE_NAME_TEMPLATE, testId);

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("partitioningType", "${partitionType}")
      .put("rangeStart", "${rangeStart}")
      .put("rangeEnd", "${rangeEnd}")
      .put("rangeInterval", "${rangeInterval}")
      .put("partitionByField", "${partitionByField}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine + "-insertWithPartitionInteger",
                        engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("partitionType", "INTEGER");
    args.put("rangeStart", "2");
    args.put("rangeEnd", "3");
    args.put("rangeInterval", "1");
    args.put("partitionByField", "int_value");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, String.format("user.%s.records.out", deploymentDetails.getSource().getName()), expectedCount, 10);
    checkMetric(tags, String.format("user.%s.records.in", deploymentDetails.getSink().getName()), expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
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

  @Test
  public void testInsertOperationWithIntegerPartitionInExistingTable() throws Exception {
    testInsertOperationWithIntegerPartitionInExistingTable(Engine.MAPREDUCE);
    testInsertOperationWithIntegerPartitionInExistingTable(Engine.SPARK);
  }

  /* Test check the Insert operation with partition type Integer and destination table exist.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Destination table:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  private void testInsertOperationWithIntegerPartitionInExistingTable(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = String.format("%s%s", SOURCE_TABLE_NAME_TEMPLATE, testId);
    String destinationTableName = String.format("%s%s", SINK_TABLE_NAME_TEMPLATE, testId);

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    RangePartitioning.Range range = RangePartitioning.Range.newBuilder()
      .setStart(2L)
      .setEnd(3L)
      .setInterval(1L)
      .build();

    RangePartitioning rangePartitioning = RangePartitioning.newBuilder()
      .setField("int_value")
      .setRange(range)
      .build();

    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(SIMPLE_FIELDS_SCHEMA);

    StandardTableDefinition standardTableDefinition = StandardTableDefinition.newBuilder()
      .setSchema(schema)
      .setRangePartitioning(rangePartitioning)
      .build();

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, standardTableDefinition);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationSourceString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("partitioningType", "${partitionType}")
      .put("rangeStart", "${rangeStart}")
      .put("rangeEnd", "${rangeEnd}")
      .put("rangeInterval", "${rangeInterval}")
      .put("partitionByField", "${partitionByField}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-insertInExistingTableWithPartitionInteger", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("partitionType", "INTEGER");
    args.put("rangeStart", "2");
    args.put("rangeEnd", "3");
    args.put("rangeInterval", "1");
    args.put("partitionByField", "int_value");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, String.format("user.%s.records.out", deploymentDetails.getSource().getName()), expectedCount,
                10);
    checkMetric(tags, String.format("user.%s.records.in", deploymentDetails.getSink().getName()), expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
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
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")),
        SIMPLE_FIELDS_SCHEMA));

    assertActualTable(destinationTableName, listValues, SIMPLE_FIELDS_SCHEMA);
  }

  @Test
  public void testInsertOperationWithIngestionTimePartition() throws Exception {
    testInsertOperationWithIngestionTimePartition(Engine.MAPREDUCE);
    testInsertOperationWithIngestionTimePartition(Engine.SPARK);
  }

  /* Test check the Insert operation with partition type Time (Ingestion Time).
   * Tables are partitioned based on the data's ingestion (load) time or arrival time.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   * Expected ending sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"boolean_value":true}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"boolean_value":false}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"boolean_value":false}
   */
  private void testInsertOperationWithIngestionTimePartition(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = String.format("%s%s", SOURCE_TABLE_NAME_TEMPLATE, testId);
    String destinationTableName = String.format("%s%s", SINK_TABLE_NAME_TEMPLATE, testId);

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("partitioningType", "${partitionType}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-insertWithIngestionTimePartition", engine);

    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("partitionType", "TIME");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, String.format("user.%s.records.out", deploymentDetails.getSource().getName()), expectedCount,
                10);
    checkMetric(tags, String.format("user.%s.records.in", deploymentDetails.getSink().getName()), expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
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

  @Test
  public void testInsertOperationWithTimeColumnPartition() throws Exception {
    testInsertOperationWithTimeColumnPartition(Engine.MAPREDUCE);
    testInsertOperationWithTimeColumnPartition(Engine.SPARK);
  }

  /* Test check the Insert operation with partition type Time (Date/timestamp/datetime).
   * Tables are partitioned based on a TIMESTAMP, DATE, or DATETIME column.
   * Input data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"date_value":"2000-01-19"}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"date_value":"2000-01-20"}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"date_value":"2000-01-21"}
   * Starting sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"date_value":"2000-01-19"}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"date_value":"2000-01-20"}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"date_value":"2000-01-21"}
   * Expected ending sink data:
   *  {"string_value":"string_1","int_value":1,"float_value":0.1,"date_value":"2000-01-19"}
   *  {"string_value":"string_2","int_value":2,"float_value":0.2,"date_value":"2000-01-20"}
   *  {"string_value":"string_3","int_value":3,"float_value":0.3,"date_value":"2000-01-21"}
   */
  private void testInsertOperationWithTimeColumnPartition(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = String.format("%s%s", SOURCE_TABLE_NAME_TEMPLATE, testId);
    String destinationTableName = String.format("%s%s", SINK_TABLE_NAME_TEMPLATE, testId);

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, TIME_PARTITION_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getTimePartitionOperationSourceString());

    Schema sourceSchema = getTimePartitionSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("partitioningType", "${partitionType}")
      .put("partitionByField", "${partitionByField}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-insertWithTimeColumnPartition", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("partitionType", "TIME");
    args.put("partitionByField", "date_value");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, String.format("user.%s.records.out", deploymentDetails.getSource().getName()), expectedCount,
                10);
    checkMetric(tags, String.format("user.%s.records.in", deploymentDetails.getSink().getName()), expectedCount, 10);

    List<FieldValueList> listValues = new ArrayList<>();
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.1"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2000-01-19")),
        TIME_PARTITION_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.2"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2000-01-20")),
        TIME_PARTITION_SCHEMA));
    listValues.add(
      FieldValueList.of(
        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string_3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.3"),
                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2000-01-21")),
        TIME_PARTITION_SCHEMA));

    assertActualTable(destinationTableName, listValues, TIME_PARTITION_SCHEMA);
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
  private void testUpdateOperationWithSchemaUpdate(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("relationTableKey", "${key}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-updateWithSchemaUpdate", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "UPDATE");
    args.put("key", "string_value");
    args.put("relax", "true");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

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

  @Test
  public void testUpsertOperationWithSchemaUpdate() throws Exception {
    testUpsertOperationWithSchemaUpdate(Engine.MAPREDUCE);
    testUpsertOperationWithSchemaUpdate(Engine.SPARK);
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
  private void testUpsertOperationWithSchemaUpdate(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationSourceString());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, UPDATE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("relationTableKey", "${key}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    int expectedCount = 3;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-upsertWithSchemaUpdate", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "UPSERT");
    args.put("key", "string_value");
    args.put("relax", "true");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

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

  @Test
  public void testUpdateOperationWithDedupeSourceData() throws Exception {
    testUpdateOperationWithDedupeSourceData(Engine.MAPREDUCE);
    testUpdateOperationWithDedupeSourceData(Engine.SPARK);
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
  private void testUpdateOperationWithDedupeSourceData(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationUpdateSourceStringWithDupe());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", "${srcSchema}")
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "UPDATE")
      .put("relationTableKey", "string_value")
      .put("dedupeBy", "int_value ASC")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 2;

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("project", getProjectId());
    runtimeArgs.put("dataset", bigQueryDataset);
    runtimeArgs.put("srcTable", sourceTableName);
    runtimeArgs.put("srcSchema", sourceSchema.toString());
    runtimeArgs.put("dstTable", destinationTableName);

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-updateWithDedupeSourceData", engine);
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, runtimeArgs);

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

  @Test
  public void testUpsertOperationWithDedupeSourceData() throws Exception {
    testUpsertOperationWithDedupeSourceData(Engine.MAPREDUCE);
    testUpsertOperationWithDedupeSourceData(Engine.SPARK);
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
  private void testUpsertOperationWithDedupeSourceData(Engine engine) throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, sourceTableName, getOperationUpsertSourceStringWithDupe());

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, destinationTableName, SIMPLE_FIELDS_SCHEMA);
    GoogleBigQueryUtils.insertData(bq, dataset, destinationTableName, getOperationDestinationWithoutUpdateString());

    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", "${srcSchema}")
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "UPSERT")
      .put("relationTableKey", "string_value")
      .put("dedupeBy", "float_value DESC")
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 3;

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("project", getProjectId());
    runtimeArgs.put("dataset", bigQueryDataset);
    runtimeArgs.put("srcTable", sourceTableName);
    runtimeArgs.put("srcSchema", sourceSchema.toString());
    runtimeArgs.put("dstTable", destinationTableName);

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + engine +
        "-upsertWithDedupeSourceData", engine);
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, runtimeArgs);

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

    com.google.cloud.bigquery.Schema expectedSchema = bq.getTable(expectedTableId).getDefinition().getSchema();
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(actualTableId).getDefinition().getSchema();

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
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(actualTableId).getDefinition().getSchema();
    List<FieldValueList> actualResult = GoogleBigQueryUtils.getResultTableData(bq, actualTableId, actualSchema);

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

    com.google.cloud.bigquery.Schema expectedSchema = bq.getTable(expectedTableId).getDefinition().getSchema();
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(actualTableId).getDefinition().getSchema();

    List<FieldValueList> expectedResult = GoogleBigQueryUtils.getResultTableData(bq, expectedTableId, expectedSchema);
    List<FieldValueList> actualResult = GoogleBigQueryUtils.getResultTableData(bq, actualTableId, actualSchema);
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

  private GoogleBigQueryTest.DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                                                 Map<String, String> sinkProperties,
                                                                 String applicationName,
                                                                 Engine engine) throws Exception {

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
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private static void createDataset() {
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

  private List<JsonObject> getTimePartitionOperationSourceString() {
    JsonObject json = new JsonObject();
    json.addProperty("string_value", "string_1");
    json.addProperty("int_value", 1);
    json.addProperty("float_value", 0.1);
    json.addProperty("date_value", "2000-01-19");
    List<JsonObject> list = new ArrayList<>();
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_2");
    json.addProperty("int_value", 2);
    json.addProperty("float_value", 0.2);
    json.addProperty("date_value", "2000-01-20");
    list.add(json);
    json = new JsonObject();
    json.addProperty("string_value", "string_3");
    json.addProperty("int_value", 3);
    json.addProperty("float_value", 0.3);
    json.addProperty("date_value", "2000-01-21");
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

  private Schema getTimePartitionSchema() {
    return Schema
      .recordOf("dateTimeSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("date_value", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)))
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
