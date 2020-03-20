/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
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
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration Test for Excel plugin.
 */
public class ExcelInputReaderTest extends ETLTestBase {
  private static final String EXCEL_PLUGIN_NAME = "Excel";

  private String sourcePath;

  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL url = new URL(serviceURL, "excelreader/create");
    //POST request to create a new file set with name excelreadersource.
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    url = new URL(serviceURL, "excelreader?path=civil_test_data_one.xlsx");
    //PUT request to upload the civil_test_data_one.xlsx file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/civil_test_data_one.xlsx"))
                              .build(), getClientConfig().getAccessToken());

    url = new URL(serviceURL, "excelreader?path=civil_test_data_two.xlsx");
    //PUT request to upload the civil_test_data_two.xlsx file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/civil_test_data_two.xlsx"))
                              .build(), getClientConfig().getAccessToken());

    URL pathServiceUrl = new URL(serviceURL, "excelreader?path");
    AccessToken accessToken = getClientConfig().getAccessToken();
    HttpResponse sourceResponse = getRestClient().execute(HttpMethod.GET, pathServiceUrl, accessToken);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, sourceResponse.getResponseCode());
    sourcePath = sourceResponse.getResponseBodyAsString();
  }

  @Test
  public void testExcelReader() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase1-testExcelInputReader")
      .put("filePath", sourcePath)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackMemoryTable")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "A:string,B:string")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .put(Properties.BatchReadableWritable.NAME, "input")
      .build();

    Schema schema = Schema.recordOf("record", Schema.Field.of("A", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("B", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("sheet", Schema.of(Schema.Type.STRING)));

    ETLStage source =
      new ETLStage("Excel", new ETLPlugin(EXCEL_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-excelreader";

    ETLStage sink = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, outputDatasetName,
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
      Properties.Table.PROPERTY_SCHEMA, schema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("ExcelReaderTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("3.0"));
    Assert.assertEquals("john", row.getString("B"));
    Assert.assertNull(row.getString("D"));
  }

  @Test
  public void testExcelReaderReprocessFalse() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase1-testExcelReaderReprocessFalse")
      .put("filePath", sourcePath)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackmemorytablereprocessfalse")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "false")
      .put("rowsLimit", "")
      .put("outputSchema", "A:string,B:string")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .put(Properties.BatchReadableWritable.NAME, "input")
      .build();

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("A", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("B", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("sheet", Schema.of(Schema.Type.STRING)));

    ETLStage source =
      new ETLStage("Excel", new ETLPlugin(EXCEL_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-testexcelreaderreprocessfalse";

    ETLStage sink = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
                                                Properties.Table.PROPERTY_SCHEMA, schema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("ExcelReaderreprocessfalse");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // Set preprocessed file data
    DataSetManager<KeyValueTable> dataSetManager = getKVTableDataset("trackmemorytablereprocessfalse");
    KeyValueTable keyValueTable = dataSetManager.get();
    String excelTestFileTwo = "civil_test_data_two.xlsx";
    File testFile = new File(sourcePath , excelTestFileTwo);
    keyValueTable.write(testFile.toURI().toString(), String.valueOf(System.currentTimeMillis()));
    dataSetManager.flush();

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("1.0"));
    Assert.assertEquals("romy", row.getString("B"));
    Assert.assertNull(row.getString("D"));
  }
}
