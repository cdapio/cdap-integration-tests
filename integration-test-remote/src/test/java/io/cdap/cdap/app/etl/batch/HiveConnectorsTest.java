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

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import io.cdap.cdap.app.etl.gcp.GoogleBigQueryUtils;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.suite.category.SDKIncompatible;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import jline.internal.Nullable;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.parquet.Strings;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Hive plugins.
 */
@Category({
  // Do not run the tests on SDK because there no Hive server available for the HivePlugin to connect to.
  SDKIncompatible.class
})
public class HiveConnectorsTest extends DataprocETLTestBase {
  private static final String FILE_PLUGIN_NAME = "File";
  private static final String HIVE_PLUGIN_NAME = "Hive";
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";

  private static final String STRING_VALUE_NAME = "string_value";
  private static final String INT_VALUE_NAME = "int_value";
  private static final String FLOAT_VALUE_NAME = "float_value";
  private static final String DOUBLE_VALUE_NAME = "double_value";
  private static final String DECIMAL_VALUE_NAME = "decimal_value";
  private static final String BOOLEAN_VALUE_NAME = "boolean_value";
  private static final String STRUCT_VALUE_NAME = "struct_value";
  private static final String MAP_VALUE_NAME = "map_value";
  private static final String ARRAY_VALUE_NAME = "array_value";
  private static final String TIMESTAMP_MILLIS_VALUE_NAME = "timestamp_millis_value";
  private static final String PARTITION_COLUMN_NAME = "partition_column";
  private static final String PARTITION_COLUMN2_NAME = "partition_column2";

  private static final String TEST_UUID = UUID.randomUUID().toString().replaceAll("-", "_");
  private static final String BIG_QUERY_DATASET_NAME = String.format("bq_dataset_%s", TEST_UUID);
  private static final String BIG_QUERY_TABLE_NAME = String.format("test_sink_table_%s", TEST_UUID);
  private static final String TEST_DATABASE_NAME = String.format("test_database_%s", TEST_UUID);
  private static final String TEST_TABLE_NAME = "test_table";
  private static final String TEMPORARY_TABLE_NAME = "temp_table";

  private static BigQuery bq;
  private static Dataset dataset;
  private String sourcePath;

  private String hiveServer;
  private int hivePort;
  private String hiveUsername;
  private String hivePassword;
  private String hiveMetastoreUrl;

  private static final Schema.Field DECIMAL_FIELD =
    Schema.Field.of(DECIMAL_VALUE_NAME,
                    Schema.decimalOf(30, 9));

  private static final Schema.Field STRUCT_FIELD =
    Schema.Field.of(STRUCT_VALUE_NAME,
                    Schema.recordOf(
                      "struct",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
                    ));

  private static final Schema.Field MAP_FIELD =
    Schema.Field.of(MAP_VALUE_NAME,
                    Schema.mapOf(
                      Schema.of(Schema.Type.STRING),
                      Schema.of(Schema.Type.STRING)
                    ));

  private static final Schema.Field ARRAY_FIELD =
    Schema.Field.of(ARRAY_VALUE_NAME,
                    Schema.arrayOf(Schema.of(Schema.Type.INT))
    );

  private static final List<Schema.Field> SCHEMA_FIELDS = ImmutableList.of(
    Schema.Field.of(STRING_VALUE_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(INT_VALUE_NAME, Schema.of(Schema.Type.INT)),
    Schema.Field.of(FLOAT_VALUE_NAME, Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of(DOUBLE_VALUE_NAME, Schema.of(Schema.Type.DOUBLE)),
    DECIMAL_FIELD,
    STRUCT_FIELD,
    MAP_FIELD,
    ARRAY_FIELD,
    Schema.Field.of(TIMESTAMP_MILLIS_VALUE_NAME, Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
    Schema.Field.of(BOOLEAN_VALUE_NAME, Schema.of(Schema.Type.BOOLEAN))
  );

  private static final Schema SCHEMA = Schema.recordOf("rec", SCHEMA_FIELDS);

  private static final Field[] EXPECTED_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder(STRING_VALUE_NAME, LegacySQLTypeName.STRING).build(),
    Field.newBuilder(INT_VALUE_NAME, LegacySQLTypeName.INTEGER).build(),
    Field.newBuilder(FLOAT_VALUE_NAME, LegacySQLTypeName.FLOAT).build(),
    Field.newBuilder(DOUBLE_VALUE_NAME, LegacySQLTypeName.FLOAT).build(),
    Field.newBuilder(DECIMAL_VALUE_NAME, LegacySQLTypeName.NUMERIC).build(),
    Field.newBuilder(STRUCT_VALUE_NAME,
                     LegacySQLTypeName.RECORD,
                     Field.newBuilder("id", LegacySQLTypeName.INTEGER).build(),
                     Field.newBuilder("name", LegacySQLTypeName.STRING).build()
    ).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder(MAP_VALUE_NAME,
                     LegacySQLTypeName.RECORD,
                     Field.newBuilder("key", LegacySQLTypeName.STRING).build(),
                     Field.newBuilder("value", LegacySQLTypeName.STRING).build()
    ).setMode(Field.Mode.REPEATED).build(),
    Field.newBuilder(ARRAY_VALUE_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build(),
    Field.newBuilder(TIMESTAMP_MILLIS_VALUE_NAME, LegacySQLTypeName.TIMESTAMP).build(),
    Field.newBuilder(BOOLEAN_VALUE_NAME, LegacySQLTypeName.BOOLEAN).build(),
  };

  @Override
  protected void innerSetup() throws Exception {

  }

  @Override
  protected void innerTearDown() throws Exception {

  }

  /**
   * Hive Source to Hive Sink test
   */
  @Test
  public void hiveSourceToHiveSinkTest() throws Exception {
    hiveSourceToHiveSinkTest(Engine.MAPREDUCE);
  }

  private void hiveSourceToHiveSinkTest(Engine engine) throws Exception {
    setUpHiveSourceEnvironment(null);

    Map<String, String> args = getHiveProperties();
    ETLStage source = getHiveSource(args);
    ETLStage sink = getHiveSink(args);
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    deployAndRun(pipelineConfig, String.format("HiveSourceToHiveSink-%s", engine.name()));
    assertSinkDataEqual();
    dropDatabase();
  }

  /**
   * Test hive source
   */
  @Test
  public void hiveSourceTest() throws Exception {
    hiveSourceTest(Engine.MAPREDUCE);
    hiveSourceTest(Engine.SPARK);
  }

  private void hiveSourceTest(Engine engine) throws Exception {
    setUpHiveSourceEnvironment(null);

    ETLBatchConfig pipelineConfig = getHiveSourceConfig(engine, getHiveProperties());
    deployAndRun(pipelineConfig, String.format("HiveSource-%s", engine.name()));

    assertSourceDataEqual();
    dropDatabase();
    deleteBigQueryDataset();
  }

  /**
   * Test hive source partition
   */
  @Test
  public void hiveSourcePartitionTest() throws Exception {
    hiveSourcePartitionTest(Engine.MAPREDUCE);
    hiveSourcePartitionTest(Engine.SPARK);
  }

  private void hiveSourcePartitionTest(Engine engine) throws Exception {
    setUpHiveSourceEnvironment(new String[]{PARTITION_COLUMN_NAME});

    ETLBatchConfig pipelineConfig = getHiveSourceConfig(engine, getHiveProperties());
    deployAndRun(pipelineConfig, String.format("HiveSource-%s", engine.name()));

    assertSourceDataEqual();
    dropDatabase();
    deleteBigQueryDataset();
  }

  /**
   * Test hive source multiple partitions
   */
  @Test
  public void hiveSourceMultiplePartitionTest() throws Exception {
    hiveSourceMultiplePartitionTest(Engine.MAPREDUCE);
    hiveSourceMultiplePartitionTest(Engine.SPARK);
  }

  private void hiveSourceMultiplePartitionTest(Engine engine) throws Exception {
    setUpHiveSourceEnvironment(new String[]{PARTITION_COLUMN_NAME, PARTITION_COLUMN2_NAME});

    ETLBatchConfig pipelineConfig = getHiveSourceConfig(engine, getHiveProperties());
    deployAndRun(pipelineConfig, String.format("HiveSource-%s", engine.name()));

    assertSourceDataEqual();
    dropDatabase();
    deleteBigQueryDataset();
  }

  /**
   * Test hive sink
   */
  @Test
  public void hiveSinkTest() throws Exception {
    hiveSinkTest(Engine.MAPREDUCE);
  }

  private void hiveSinkTest(Engine engine) throws Exception {
    setUpHiveSinkEnvironment(null, "hive_source.json");

    ETLBatchConfig pipelineConfig = getHiveSinkConfig(engine, getHiveProperties(), SCHEMA);
    deployAndRun(pipelineConfig, String.format("HiveSink-%s", engine.name()));

    assertSinkDataEqual();
    dropDatabase();
  }

  /**
   * Test hive sink dynamic partition
   */
  @Test
  public void hiveSinkDynamicPartitionTest() throws Exception {
    hiveSinkDynamicPartitionTest(Engine.MAPREDUCE);
  }

  private void hiveSinkDynamicPartitionTest(Engine engine) throws Exception {
    setUpHiveSinkEnvironment(new String[]{PARTITION_COLUMN_NAME}, "hive_source_partition.json");

    ETLBatchConfig pipelineConfig = getHiveSinkConfig(engine, getHiveProperties(), getPartitionedTableSchema());
    deployAndRun(pipelineConfig, String.format("HiveSink-%s", engine.name()));

    assertSinkDataEqual();
    dropDatabase();
  }

  /**
   * Test hive sink static partition
   */
  @Test
  public void hiveSinkStaticPartitionTest() throws Exception {
    hiveSinkStaticPartitionTest(Engine.MAPREDUCE);
  }

  private void hiveSinkStaticPartitionTest(Engine engine) throws Exception {
    setUpHiveSinkEnvironment(new String[]{PARTITION_COLUMN_NAME}, "hive_source.json");

    Map<String, String> args = getHiveProperties();
    args.put("partitionFilter", "{\"partition_column\": \"TE\"}");

    ETLBatchConfig pipelineConfig = getHiveSinkConfig(engine, args, SCHEMA);
    deployAndRun(pipelineConfig, String.format("HiveSink-%s", engine.name()));

    assertSinkDataEqual();
    dropDatabase();
  }

  /**
   * Test hive sink multiple partitions
   */
  @Test
  public void hiveSinkMultiplePartitionsTest() throws Exception {
    hiveSinkMultiplePartitionsTest(Engine.MAPREDUCE);
  }

  private void hiveSinkMultiplePartitionsTest(Engine engine) throws Exception {
    setUpHiveSinkEnvironment(new String[]{PARTITION_COLUMN_NAME, PARTITION_COLUMN2_NAME},
                             "hive_source_multiple_partitions.json");

    ETLBatchConfig pipelineConfig = getHiveSinkConfig(engine, getHiveProperties(), getMultiplePartitionsTableSchema());
    deployAndRun(pipelineConfig, String.format("HiveSink-%s", engine.name()));

    assertSinkDataEqual();
    dropDatabase();
  }

  private void deployAndRun(ETLBatchConfig config, String name) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app(name);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    Map<String, String> args = new HashMap<>();
    args.put("metastoreUrl", hiveMetastoreUrl);
    args.put("username", "hive");
    args.put("password", "hive");
    args.put("database", TEST_DATABASE_NAME);
    args.put("table", TEST_TABLE_NAME);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, args, 20, TimeUnit.MINUTES);
  }

  private ETLBatchConfig getHiveSourceConfig(Engine engine, Map<String, String> sourceProps) {
    ETLStage source = getHiveSource(sourceProps);
    ETLStage sink = getBigQuerySink();
    return ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
  }

  private ETLBatchConfig getHiveSinkConfig(Engine engine, Map<String, String> sinkProps, Schema schema) {
    ETLStage fileSource = getFileSource(sourcePath, schema);
    ETLStage hiveSink = getHiveSink(sinkProps);

    return ETLBatchConfig.builder()
      .setEngine(engine)
      .addStage(fileSource)
      .addStage(hiveSink)
      .addConnection(fileSource.getName(), hiveSink.getName())
      .build();
  }

  private ETLStage getHiveSource(Map<String, String> args) {
    return new ETLStage("HiveSource", new ETLPlugin(HIVE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, args, null));
  }

  private ETLStage getBigQuerySink() {
    Map<String, String> props = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", BIG_QUERY_DATASET_NAME)
      .put("table", BIG_QUERY_TABLE_NAME)
      .put("operation", "INSERT")
      .put("allowSchemaRelaxation", "false")
      .build();

    return new ETLStage("BigQuerySinkStage",
                        new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, props, GOOGLE_CLOUD_ARTIFACT));
  }

  private ETLStage getFileSource(String path, Schema schema) {
    Map<String, String> args = new HashMap<>();
    args.put("referenceName", "ref");
    args.put("format", "json");
    args.put("schema", schema.toString());
    args.put("path", path);

    return new ETLStage("FileSource", new ETLPlugin(FILE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, args, null));
  }

  private ETLStage getHiveSink(Map<String, String> args) {
    return new ETLStage("HiveSink", new ETLPlugin(HIVE_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, args, null));
  }

  private Map<String, String> getHiveProperties() {
    Map<String, String> args = new HashMap<>();
    args.put("metastoreUrl", "${metastoreUrl}");
    args.put("metastoreUsername", "${username}");
    args.put("metastorePassword", "${password}");
    args.put("database", "${database}");
    args.put("table", "${table}");
    args.put("referenceName", "ref");
    return args;
  }

  private static Schema getPartitionedTableSchema() {
    List<Schema.Field> fields = new ArrayList<>(SCHEMA_FIELDS);
    fields.add(Schema.Field.of(PARTITION_COLUMN_NAME, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf("table_schema", fields);
  }

  private static Schema getMultiplePartitionsTableSchema() {
    List<Schema.Field> fields = new ArrayList<>(SCHEMA_FIELDS);
    fields.add(Schema.Field.of(PARTITION_COLUMN_NAME, Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of(PARTITION_COLUMN2_NAME, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf("table_schema", fields);
  }

  private static List<FieldValueList> getExpectedValues() {
    return ImmutableList.of(FieldValueList.of(
      Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Zoe"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "869874622"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "82.022499084472656"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "7763.959904"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "6828.293003"),
                    FieldValue.of(FieldValue.Attribute.RECORD,
                                  FieldValueList.of(
                                    ImmutableList.of(
                                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                                      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Zoe")
                                    ),
                                    Field.of("name", LegacySQLTypeName.STRING),
                                    Field.of("id", LegacySQLTypeName.INTEGER)
                                  )),
                    FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(
                      ImmutableList.of(FieldValue.of(
                        FieldValue.Attribute.RECORD,
                        FieldValueList.of(
                          ImmutableList.of(
                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "name"),
                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Zoe")
                          ),
                          Field.of("key", LegacySQLTypeName.STRING),
                          Field.of("value", LegacySQLTypeName.STRING)))),
                      Field.of("el", LegacySQLTypeName.STRING))),
                    FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(
                      ImmutableList.of(
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "3")
                      ),
                      Field.of("el1", LegacySQLTypeName.STRING),
                      Field.of("el2", LegacySQLTypeName.STRING),
                      Field.of("el3", LegacySQLTypeName.STRING))),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1622455055.628"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")
      ),
      EXPECTED_FIELDS_SCHEMA));
  }

  public static List<FieldValueList> getResultTableData(BigQuery bq, TableId tableId,
                                                        com.google.cloud.bigquery.Schema schema) {
    TableResult tableResult = bq.listTableData(tableId, schema);
    List<FieldValueList> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::add);
    return result;
  }

  private void assertSourceDataEqual() {
    List<FieldValueList> expectedResult = getExpectedValues();
    TableId actualTableId = TableId.of(BIG_QUERY_DATASET_NAME, BIG_QUERY_TABLE_NAME);
    com.google.cloud.bigquery.Schema actualSchema = bq.getTable(actualTableId).getDefinition().getSchema();
    List<FieldValueList> actualResult = getResultTableData(bq, actualTableId, actualSchema);

    for (int i = 0; i < expectedResult.size(); i++) {
      FieldValueList expectedFieldValueList = expectedResult.get(i);
      FieldValueList actualFieldValueList = actualResult.get(i);
      Arrays.stream(EXPECTED_FIELDS_SCHEMA).map(Field::getName)
        .forEach(fieldName -> {
          FieldValue expected = expectedFieldValueList.get(fieldName);
          FieldValue actual = actualFieldValueList.get(fieldName);
          Assert.assertEquals(expected, actual);
        });
    }
  }

  private void assertSinkDataEqual() throws Exception {
    RowSet results = queryTable(String.format("SELECT * FROM %s.%s LIMIT 1", TEST_DATABASE_NAME, TEST_TABLE_NAME));
    for (Object[] result : results) {
      Assert.assertTrue(Arrays.asList(result).get(0).toString().equalsIgnoreCase("Zoe"));
      Assert.assertEquals(869874622, Arrays.asList(result).get(1));
      // ThriftCLIServiceClient returns float values as double
      Assert.assertEquals(82.0225d, Arrays.asList(result).get(2));
      Assert.assertEquals(7763.959904d, Arrays.asList(result).get(3));
      Assert.assertEquals("6828.293003", Arrays.asList(result).get(4));
      Assert.assertEquals("{\"id\":1,\"name\":\"Zoe\"}", Arrays.asList(result).get(5));
      Assert.assertEquals("{\"name\":\"Zoe\"}", Arrays.asList(result).get(6));
      Assert.assertEquals("[1,2,3]", Arrays.asList(result).get(7));
      Assert.assertEquals("2021-05-31 11:57:35.628", Arrays.asList(result).get(8));
      Assert.assertFalse((Boolean) Arrays.asList(result).get(9));
    }
  }

  private void hiveConnectionSetUp() throws NumberFormatException {
    hiveServer = System.getProperty("hiveServer");
    hivePort = Integer.parseInt(System.getProperty("hivePort"));
    hiveUsername = System.getProperty("hiveUsername");
    hivePassword = System.getProperty("hivePassword");
    hiveMetastoreUrl = System.getProperty("hiveMetastoreUrl");

    if (Strings.isNullOrEmpty(hiveServer) || Strings.isNullOrEmpty(hiveUsername) ||
      Strings.isNullOrEmpty(hivePassword) || Strings.isNullOrEmpty(hiveMetastoreUrl)) {
      throw new IllegalArgumentException("Missing configuration for Hive server.");
    }
  }

  private void setUpHiveSourceEnvironment(@Nullable String[] partitions) throws Exception {
    //TODO - Uncomment the following line and fill the required params when plugin will be available in hub.
    // installPluginFromHub("package-name", "plugin-name", "plugin-version");
    hiveConnectionSetUp();
    setUpBigQuery();
    setupHiveDatabase(partitions);
    populateHiveTable(partitions);
  }

  private void setUpHiveSinkEnvironment(@Nullable String[] partitions, String filePath) throws Exception {
    //TODO - Uncomment the following line and fill the required params when plugin will be available in hub.
    // installPluginFromHub("package-name", "plugin-name", "plugin-version");
    hiveConnectionSetUp();
    setPath(filePath);
    setupHiveDatabase(partitions);
  }

  private void setupHiveDatabase(String[] partitions) throws Exception {
    String createTableStatement =
      String.format(
        "CREATE TABLE %s.%s(" +
          "%s string, " +
          "%s int, " +
          "%s float, " +
          "%s double, " +
          "%s decimal(12,6), " +
          "%s struct<id:int,name:string>, " +
          "%s map<string,string>, " +
          "%s array<int>, " +
          "%s timestamp, " +
          "%s boolean) %s",
        TEST_DATABASE_NAME,
        TEST_TABLE_NAME,
        STRING_VALUE_NAME,
        INT_VALUE_NAME,
        FLOAT_VALUE_NAME,
        DOUBLE_VALUE_NAME,
        DECIMAL_VALUE_NAME,
        STRUCT_VALUE_NAME,
        MAP_VALUE_NAME,
        ARRAY_VALUE_NAME,
        TIMESTAMP_MILLIS_VALUE_NAME,
        BOOLEAN_VALUE_NAME,
        partitions == null ? "" : createPartitionedTable(partitions)
      );

    String createTemporaryTableStatement =
      String.format("CREATE TABLE %s.%s(id INT, name STRING)", TEST_DATABASE_NAME, TEMPORARY_TABLE_NAME);
    String addDummyData =
      String.format("INSERT INTO %s.%s VALUES(1, \"Test\")", TEST_DATABASE_NAME, TEMPORARY_TABLE_NAME);

    executeStatement(String.format("CREATE DATABASE %s", TEST_DATABASE_NAME));
    executeStatement(createTemporaryTableStatement);
    executeStatement(addDummyData);
    executeStatement(createTableStatement);
  }

  private String createPartitionedTable(String[] partitions) {
    String s = "PARTITIONED BY (";
    for (String partition : partitions) {
      if (partition.equals(partitions[partitions.length - 1])) {
        s = s.concat(String.format("%s STRING)", partition));
      } else {
        s = s.concat(String.format("%s STRING, ", partition));
      }
    }
    return s;
  }

  private String insertIntoPartitionedTable(String[] partitions) {
    String s = "PARTITION (";

    for (String partition : partitions) {
      if (partition.equals(partitions[partitions.length - 1])) {
        s = s.concat(String.format("%s=\"US\")", partition));
      } else {
        s = s.concat(String.format("%s=\"NY\", ", partition));
      }
    }
    return s;
  }

  private void populateHiveTable(@Nullable String[] partitions) throws Exception {
    String statement =
      String.format("INSERT INTO %s.%s %s select " +
                      "\"Zoe\", " +
                      "869874622, " +
                      "82.0225, " +
                      "7763.959904, " +
                      "6828.293003, " +
                      "(named_struct('id', 1, 'name', \"Zoe\")), " +
                      "(map(\"name\",\"Zoe\")), " +
                      "(array(1, 2, 3)), " +
                      "1622462255628, " +
                      "false " +
                      "from %s.%s limit 1",
                    TEST_DATABASE_NAME,
                    TEST_TABLE_NAME,
                    partitions == null ? "" : insertIntoPartitionedTable(partitions),
                    TEST_DATABASE_NAME,
                    TEMPORARY_TABLE_NAME
      );
    executeStatement(statement);
  }

  private RowSet queryTable(String statement) throws IOException, TTransportException, HiveSQLException {
    TTransport transport = new TSocket(hiveServer, hivePort);
    transport = PlainSaslHelper.getPlainTransport(hiveUsername, hivePassword, transport);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    transport.open();
    ThriftCLIServiceClient cliServiceClient = new ThriftCLIServiceClient(new TCLIService.Client(protocol));
    SessionHandle sessionHandle = cliServiceClient.openSession(hiveUsername, hivePassword);
    OperationHandle operationHandle =
      cliServiceClient.executeStatement(sessionHandle, statement, null);
    RowSet results = cliServiceClient.fetchResults(operationHandle);
    transport.close();
    return results;
  }

  private void setUpBigQuery() throws IOException {
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(BIG_QUERY_DATASET_NAME).build();
    dataset = bq.create(datasetInfo);
  }

  private static void deleteBigQueryDataset() {
    bq.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
  }

  private void dropDatabase() throws HiveSQLException, TTransportException, IOException {
    executeStatement(String.format("DROP DATABASE %s CASCADE", TEST_DATABASE_NAME));
  }

  private void executeStatement(String statement) throws IOException, TTransportException, HiveSQLException {
    TTransport transport = new TSocket(hiveServer, hivePort);
    transport = PlainSaslHelper.getPlainTransport(hiveUsername, hivePassword, transport);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    transport.open();
    ThriftCLIServiceClient cliServiceClient = new ThriftCLIServiceClient(new TCLIService.Client(protocol));
    SessionHandle sessionHandle = cliServiceClient.openSession(hiveUsername, hivePassword);
    cliServiceClient.executeStatement(sessionHandle, statement, null);
    transport.close();
  }

  private void setPath(String fileName) throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL url = new URL(serviceURL, "test_source/create");
    //POST request to create a new file set with name test_source.
    HttpResponse response = getRestClient()
      .execute(HttpRequest.post(url).withBody("").build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    url = new URL(serviceURL, String.format("test_source?path=%s", fileName));
    //PUT request to upload the civil_test_data_one.xlsx file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File(String.format("src/test/resources/%s", fileName)))
                              .build(), getClientConfig().getAccessToken());

    URL pathServiceUrl = new URL(serviceURL, "test_source?path");
    AccessToken accessToken = getClientConfig().getAccessToken();
    HttpResponse sourceResponse = getRestClient().execute(HttpMethod.GET, pathServiceUrl, accessToken);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, sourceResponse.getResponseCode());
    sourcePath = sourceResponse.getResponseBodyAsString();
  }
}
