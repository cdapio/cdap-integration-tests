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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
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
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("1.0"));
    Assert.assertEquals("romy", row.getString("B"));
    Assert.assertNull(row.getString("D"));
  }
}
