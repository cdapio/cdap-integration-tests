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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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

  private URL serviceURL;
  private String sourcePath;
  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    serviceManager.start();
    serviceManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 120);
    serviceURL = serviceManager.getServiceURL();

    URL url = new URL(serviceURL, "excelreader/create");
    //POST request to create a new file set with name excelreadersource.
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    url = new URL(serviceURL, "excelreader?path=civil_test_data_one.xlsx");
    //PUT request to upload the civil_test_data_one.xlsx file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/civil_test_data_one.xlsx"))
                              .build(), getClientConfig().getAccessToken(), HttpURLConnection.HTTP_OK);

    url = new URL(serviceURL, "excelreader?path=civil_test_data_two.xlsx");
    //PUT request to upload the civil_test_data_two.xlsx file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/civil_test_data_two.xlsx"))
                              .build(), getClientConfig().getAccessToken(), HttpURLConnection.HTTP_OK);

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

    ETLStage source = new ETLStage("Excel", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-excelreader";
    ETLStage transform = new ETLStage("transform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                              ImmutableMap.of("schema", schema.toString()), null));

    ETLStage sink = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, outputDatasetName,
      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
      Properties.Table.PROPERTY_SCHEMA, schema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExcelReaderTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("3.0"));
    Assert.assertEquals("john", row.getString("B"));
    Assert.assertNull(row.getString("D"));
  }

  @Test
  public void testTerminateOnEmptyRow() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase-TerminateOnEmptyRow")
      .put("filePath", sourcePath)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "trackmemorytablewithterminateonemptyrow")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "true")
      .put("rowsLimit", "10")
      .put("outputSchema", "A:string,B:string")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .put(Properties.BatchReadableWritable.NAME, "input")
      .build();

    Schema schema = Schema.recordOf("record", Schema.Field.of("A", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("B", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("sheet", Schema.of(Schema.Type.STRING)));

    co.cask.cdap.etl.common.ETLStage source =
      new co.cask.cdap.etl.common.ETLStage("Excel", new Plugin("Excel", sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-terminateonemptyrow";

    Map<String, String> transformProperties = ImmutableMap.of(Properties.Table.PROPERTY_SCHEMA, schema.toString());
    co.cask.cdap.etl.common.ETLStage transform =
      new co.cask.cdap.etl.common.ETLStage("transform", new Plugin("Projection", transformProperties, null));

    Map<String, String> sinkProperties = ImmutableMap.of(Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                         Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
                                                         Properties.Table.PROPERTY_SCHEMA, schema.toString());
    co.cask.cdap.etl.common.ETLStage sink =
      new co.cask.cdap.etl.common.ETLStage("TableSink", new Plugin("Table", sinkProperties, null));

    co.cask.cdap.etl.batch.config.ETLBatchConfig etlBatchConfig =
      new co.cask.cdap.etl.batch.config.ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));

    AppRequest<co.cask.cdap.etl.batch.config.ETLBatchConfig> request = getBatchAppRequest(etlBatchConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExcelReaderTest-terminateonemptyrow");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(10, TimeUnit.MINUTES);
    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());
  }

  @Test
  public void testTerminateOnErrorRecord() throws Exception {
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("filePath", sourcePath)
      .put("filePattern", ".*")
      .put("sheet", "Sheet Name")
      .put("sheetValue", "Sheet1")
      .put("memoryTableName", "tracktemorytablewithterminateonerrorrecord")
      .put("tableExpiryPeriod", "30")
      .put("reprocess", "false")
      .put("columnList", "")
      .put("columnMapping", "")
      .put("skipFirstRow", "false")
      .put("terminateIfEmptyRow", "true")
      .put("rowsLimit", "10")
      .put("outputSchema", "A:string,B:long")
      .put("ifErrorRecord", "Ignore error and continue")
      .put("errorDatasetName", "")
      .put(Properties.BatchReadableWritable.NAME, "input")
      .build();

    Schema schema = Schema.recordOf("record", Schema.Field.of("A", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("B", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("file", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("sheet", Schema.of(Schema.Type.STRING)));

    co.cask.cdap.etl.common.ETLStage source = new
      co.cask.cdap.etl.common.ETLStage("Excel", new Plugin("Excel", sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-terminateonerrorrecord";

    Map<String, String> transformProperties = ImmutableMap.of(Properties.Table.PROPERTY_SCHEMA, schema.toString());
    co.cask.cdap.etl.common.ETLStage transform =
      new co.cask.cdap.etl.common.ETLStage("transform", new Plugin("Projection", transformProperties, null));

    Map<String, String> sinkProperties = ImmutableMap.of(Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                         Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
                                                         Properties.Table.PROPERTY_SCHEMA, schema.toString());

    co.cask.cdap.etl.common.ETLStage sink =
      new co.cask.cdap.etl.common.ETLStage("TableSink", new Plugin("Table", sinkProperties, null));

    co.cask.cdap.etl.batch.config.ETLBatchConfig etlBatchConfig =
      new co.cask.cdap.etl.batch.config.ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));

    AppRequest<co.cask.cdap.etl.batch.config.ETLBatchConfig> request = getBatchAppRequest(etlBatchConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExcelReaderTest-terminateonerrorrecord");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(10, TimeUnit.MINUTES);
    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());
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

    ETLStage source = new ETLStage("Excel", new ETLPlugin("Excel", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest-testexcelreaderreprocessfalse";

    ETLStage transform =
      new ETLStage("ProjectionTransform2", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                         ImmutableMap.of("schema", schema.toString()), null));

    ETLStage sink = new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "A",
                                                Properties.Table.PROPERTY_SCHEMA, schema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = NamespaceId.DEFAULT.app("ExcelReaderreprocessfalse");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

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
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("1.0"));
    Assert.assertEquals("romy", row.getString("B"));
    Assert.assertNull(row.getString("D"));
  }
}
