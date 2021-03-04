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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.common.UnauthenticatedException;
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
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.parquet.Strings;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for CSV Schema Detection Plugin
 */
public class FileTest extends ETLTestBase {
  private static final String BODY_PATH = "src/test/resources/schema-detection-data/";

  private enum DirectoryType {
    MULTIPLE_FILES_WITH_HEADERS_AND_SIMILAR_SCHEMAS,
    MULTIPLE_FILES_WITH_HEADERS_AND_DIFFERENT_SCHEMAS,
    MULTIPLE_FILES_WITHOUT_HEADERS_AND_SIMILAR_SCHEMAS,
    MULTIPLE_FILES_WITHOUT_HEADERS_AND_DIFFERENT_SCHEMAS
  }

  private void uploadFile(String fileName, String prefix, URL serviceURL) {
    try {
      URL url = new URL(serviceURL, "testFileSet?path=" + fileName);
      getRestClient().execute(HttpRequest.put(url).withBody(new File(BODY_PATH + prefix + fileName))
                                .build(), getClientConfig().getAccessToken());
    } catch (MalformedURLException e1) {
      throw new RuntimeException("Failed to create URL! Please check if the URL is formed correctly.", e1);
    } catch (IOException | UnauthenticatedException e2) {
      throw new RuntimeException(String.format("Failed uploading file {} in CDAP!", fileName), e2);
    }
  }

  private String setupData(DirectoryType directoryType) throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL url = new URL(serviceURL, "testFileSet/create");
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    URL pathServiceUrl = new URL(serviceURL, "testFileSet?path");
    AccessToken accessToken = getClientConfig().getAccessToken();
    HttpResponse sourceResponse = getRestClient().execute(HttpMethod.GET, pathServiceUrl, accessToken);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, sourceResponse.getResponseCode());
    String sourcePath = sourceResponse.getResponseBodyAsString();

    switch (directoryType) {
      case MULTIPLE_FILES_WITH_HEADERS_AND_SIMILAR_SCHEMAS:
        Arrays.asList("test-data-1.csv", "test-data-2.csv").forEach(fileName -> uploadFile(
          fileName, "multiple-files-with-headers-and-similar-schemas/", serviceURL));
        break;

      case MULTIPLE_FILES_WITH_HEADERS_AND_DIFFERENT_SCHEMAS:
        Arrays.asList("test-data-3.csv", "test-data-4.csv").forEach(fileName -> uploadFile(
          fileName, "multiple-files-with-headers-and-different-schemas/", serviceURL));
        break;

      case MULTIPLE_FILES_WITHOUT_HEADERS_AND_SIMILAR_SCHEMAS:
        Arrays.asList("test-data-5.csv", "test-data-6.csv").forEach(fileName -> uploadFile(
          fileName, "multiple-files-without-headers-and-similar-schemas/", serviceURL));
        break;

      case MULTIPLE_FILES_WITHOUT_HEADERS_AND_DIFFERENT_SCHEMAS:
        Arrays.asList("test-data-7.csv", "test-data-8.csv").forEach(fileName -> uploadFile(
          fileName, "multiple-files-without-headers-and-different-schemas/", serviceURL));
        break;
    }
    return sourcePath;
  }

  private ETLPlugin createFileSourcePlugin(String inputSchema, String format, String delimiter,
                                           String sampleSize, String override, String referenceName,
                                           String skipHeader, String sourcePath, String fileRegex) {

    Map<String, String> args = new HashMap<>();
    args.put("referenceName", referenceName);
    args.put("schema", inputSchema);
    args.put("path", sourcePath);
    args.put("format", format);
    args.put("delimiter", delimiter);
    args.put("sampleSize", sampleSize);
    args.put("skipHeader", skipHeader);

    if (!Strings.isNullOrEmpty(fileRegex)) {
      args.put("fileRegex", fileRegex);
    }

    if (!Strings.isNullOrEmpty(override)) {
      args.put("override", override);
    }

    return new ETLPlugin("File", BatchSource.PLUGIN_TYPE, args, null);
  }

  private ETLPlugin createTableSinkPlugin(String outputSchema, String schemaRowField, String tableName) {
    Map<String, String> args = new ImmutableMap.Builder<String, String>()
      .put("schema", outputSchema)
      .put("schema.row.field", schemaRowField)
      .put("name", tableName)
      .build();

    return new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, args, null);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testFirstAutomaticSchemaDetection() throws Exception {
    /*
     *  Run automatic schema detection in a directory of multiple files, with headers, and similar schemas.
     *   - Filter file regex is set to ".+one.*" so only "test-data-1.csv" if considered.
     *   - Headers are taken from the data file.
     *   - We manually set the data type of column "double_col2" to string.
     *   - Schema is set to "" in order to trigger the automated schema detection.
     */

    String inputSchema = "";
    String regex = ".+1\\.csv";
    String skipHeaders = "true";
    String override = "double_col2:string";

    String sourcePath = setupData(DirectoryType.MULTIPLE_FILES_WITH_HEADERS_AND_SIMILAR_SCHEMAS);

    // The source plugin
    ETLPlugin fileSourcePlugin = createFileSourcePlugin(inputSchema, "delimited", ";", "5",
                                                        override, "File", skipHeaders, sourcePath, regex);

    ETLStage source = new ETLStage("FileBatchSource", fileSourcePlugin);

    // The sink plugin
    String tableName = "DummyTable";
    Schema outputSchema = Schema.recordOf("etlSchemaBody",
                                          Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("string_col", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("int_col", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("double_col1", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("double_col2", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("long_col", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("date_col", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("time_col", Schema.of(Schema.Type.STRING))
    );

    String rowKeyColumn = "id";
    ETLPlugin sinkTablePlugin = createTableSinkPlugin(outputSchema.toString(), rowKeyColumn, tableName);
    ETLStage sink = new ETLStage("TableSink", sinkTablePlugin);

    // Pipeline
    ETLBatchConfig pipelineConfig = ETLBatchConfig
      .builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("Schema-Detection-Test");
    AppRequest appRequest = getBatchAppRequestV2(pipelineConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    SparkManager sparkManager = appManager.getSparkManager("SchemaDetectionTest");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> fullArgs = new HashMap<>();
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, fullArgs, 5, TimeUnit.MINUTES);

    DataSetManager<Table> tableManager = getTableDataset(tableName);
    Table table = tableManager.get();

    /*
     * Methods as getString(val), getInt(val), getDouble(val),..., get the value successfully, if type(value)
     * is String, Int, Double,..., respectively. Otherwise, exception are thrown. For example if the data type of
     * "columnA" is Integer, then getInt(columnA) returns the integer value correctly whereas getString(columnA)
     * will hit an exception.
     */

    Row firstRow = table.get(Bytes.toBytes(1));
    Assert.assertEquals("colorado", firstRow.getString("string_col"));
    Assert.assertEquals(new Integer(100), firstRow.getInt("int_col"));
    Assert.assertEquals(new Double(0.001), firstRow.getDouble("double_col1"));
    Assert.assertEquals("0.001", firstRow.getString("double_col2"));
    Assert.assertEquals(new Long(1111111111111L), firstRow.getLong("long_col"));

    // Here we prove that when regex file filter is set, the File Source Plugin does not read test-data-2.csv
    Row sixthRow = table.get(Bytes.toBytes(6));
    Assert.assertNull(sixthRow.getString("string_col"));

    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("exceed the capacity of the array");
    firstRow.getDouble("double_col2"); // throws an exception since type(double_col2) is String
  }

  @Test
  public void testSecondAutomaticSchemaDetection() throws Exception {
    /*
     * Run automatic schema detection in a directory of multiple files, without headers, and similar schemas.
     *   - Data files do not have headers. Hence, headers are automatically generated as body_0,..., body_k.
     *   - File filter regex is not specified (null). Hence, both files "test-data-5.csv" and "test-data-6.csv"
     *     are fetched by the source plugin.
     *   - Override is set to null. The automatic schema detection detects the schema for all the columns.
     */

    String sourcePath = setupData(DirectoryType.MULTIPLE_FILES_WITHOUT_HEADERS_AND_SIMILAR_SCHEMAS);
    String skipHeader = "false";
    String regex = null;
    String override = null;

    // The source plugin
    ETLPlugin fileSourcePlugin = createFileSourcePlugin("", "delimited", ";", "5",
                                                        override, "File", skipHeader, sourcePath, regex);

    ETLStage source = new ETLStage("FileBatchSource", fileSourcePlugin);

    // The sink plugin
    String tableName = "DummyTable";
    Schema outputSchema = Schema.recordOf("etlSchemaBody",
                                          Schema.Field.of("body_0", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("body_1", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("body_2", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("body_3", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("body_4", Schema.of(Schema.Type.LONG)));

    String rowKeyColumn = "body_0";
    ETLPlugin sinkTablePlugin = createTableSinkPlugin(outputSchema.toString(), rowKeyColumn, tableName);
    ETLStage sink = new ETLStage("TableSink", sinkTablePlugin);

    // Pipeline
    ETLBatchConfig pipelineConfig = ETLBatchConfig
      .builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("Schema-Detection-Test");
    AppRequest appRequest = getBatchAppRequestV2(pipelineConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    SparkManager sparkManager = appManager.getSparkManager("SchemaDetectionTest");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> fullArgs = new HashMap<>();
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, fullArgs, 5, TimeUnit.MINUTES);

    DataSetManager<Table> tableManager = getTableDataset(tableName);
    Table table = tableManager.get();

    Row firstRow = table.get(Bytes.toBytes(1));
    Assert.assertEquals("colorado", firstRow.getString("body_1"));
    Assert.assertEquals(new Integer(100), firstRow.getInt("body_2"));
    Assert.assertEquals(new Double(0.001), firstRow.getDouble("body_3"));
    Assert.assertEquals(new Long(1111111111111L), firstRow.getLong("body_4"));

    // Here we prove that "test-data-6.csv" is also read
    Row sixthRow = table.get(Bytes.toBytes(6));
    Assert.assertEquals("washington dc", sixthRow.getString("body_1"));
  }

  @Test
  public void testThirdAutomaticSchemaDetectionWithHeaders() throws Exception {
    // Run automatic schema detection in a directory of multiple files, with headers and different schemas.
    String sourcePath = setupData(DirectoryType.MULTIPLE_FILES_WITH_HEADERS_AND_DIFFERENT_SCHEMAS);
    testThirdAutomaticSchemaDetection("true", sourcePath);
  }

  @Test
  public void testThirdAutomaticSchemaDetectionWithoutHeaders() throws Exception {
    // Run automatic schema detection in a directory of multiple files, without headers and different schemas.
    String sourcePath = setupData(DirectoryType.MULTIPLE_FILES_WITHOUT_HEADERS_AND_DIFFERENT_SCHEMAS);
    testThirdAutomaticSchemaDetection("false", sourcePath);
  }

  private void testThirdAutomaticSchemaDetection(String skipHeaders, String sourcePath) throws Exception {
    /*
     *  - Filter file regex is set to "null". The automated schema detection plugin will pickup the schema from
     *    the first file listed, and use that schema to read the other files. The plugin will fail to do so, since
     *    other files have different schema.
     *  - Schema is set to "" in order to trigger the automated schema detection.
     */

    String inputSchema = "";
    String regex = null;
    String override = null;

    // The source plugin
    ETLPlugin fileSourcePlugin = createFileSourcePlugin(inputSchema, "delimited", ";", "5",
                                                        override, "File", skipHeaders, sourcePath, regex);

    ETLStage source = new ETLStage("FileBatchSource", fileSourcePlugin);

    // The sink plugin
    String tableName = "DummyTable";
    String colNameOne = "id";
    String colNameTwo = "city";

    if (skipHeaders.equals("false")) {
      colNameOne = "body_0";
      colNameTwo = "body_1";
    }

    Schema outputSchema = Schema.recordOf(
      "etlSchemaBody",
      Schema.Field.of(colNameOne, Schema.of(Schema.Type.INT)),
      Schema.Field.of(colNameTwo, Schema.of(Schema.Type.STRING))
    );

    ETLPlugin sinkTablePlugin = createTableSinkPlugin(outputSchema.toString(), colNameOne, tableName);
    ETLStage sink = new ETLStage("TableSink", sinkTablePlugin);

    // Pipeline
    ETLBatchConfig pipelineConfig = ETLBatchConfig
      .builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("Schema-Detection-Test");
    AppRequest appRequest = getBatchAppRequestV2(pipelineConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    SparkManager sparkManager = appManager.getSparkManager("SchemaDetectionTest");

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> fullArgs = new HashMap<>();

    /*
     * The pipeline will fail intentionally. This because, we have a directory of files with different schemas. The
     * automatic schema detection plugin detect the schema of only one single file and assumes that other files
     * have similar schemas. Hence, when the File Source Plugin reads the other files with incompatible schemas,
     * exceptions get thrown.
     */
    startAndWaitForRun(workflowManager, ProgramRunStatus.FAILED, fullArgs, 5, TimeUnit.MINUTES);

    DataSetManager<Table> tableManager = getTableDataset(tableName);
    Table table = tableManager.get();

    // Assert that table has no records present
    Assert.assertEquals(0, table.get(Bytes.toBytes(1)).getColumns().size());
  }
}
