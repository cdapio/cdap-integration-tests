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
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
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
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for XMLReaderBatchSource.
 */
public class XMLReaderTest extends ETLTestBase {
  private static final Schema TRANSFORM_SCHEMA = Schema.recordOf(
    "xmlTransform",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING)));
  private static final Schema SINK_SCHEMA = Schema.recordOf(
    "xmlSink",
    Schema.Field.of("offset", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("record", Schema.of(Schema.Type.STRING)));

  private String sourcePath;
  private String targetPath;
  private URL serviceURL;

  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(UploadFile.
                                                                           FileSetService.class.getSimpleName());
    serviceManager.start();
    serviceManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 120);

    serviceURL = serviceManager.getServiceURL();
    URL url = new URL(serviceURL, "xmlreadersource/create");
    //POST request to create a new file set with name xmlreadersource.
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    url = new URL(serviceURL, "xmlreadersource?path=catalog.xml");
    //PUT request to upload the catalog.xml file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/catalog.xml")).build(),
                            getClientConfig().getAccessToken(), HttpURLConnection.HTTP_OK);
    url = new URL(serviceURL, "xmlreadersource?path=catalogProcessedFile.xml");
    //PUT request to upload the catalogProcessedFile.xml file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/catalogProcessedFile.xml"))
                              .build(), getClientConfig().getAccessToken(), HttpURLConnection.HTTP_OK);

    url = new URL(serviceURL, "xmlreadertarget/create");
    //POST request to create a new file set with name xmlreadertarget.
    response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    //GET location of the fileset xmlreadersource on cluster.
    url = new URL(serviceURL, "xmlreadersource?path");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    sourcePath = response.getResponseBodyAsString();

    //GET location of the fileset xmlreadertarget on cluster.
    url = new URL(serviceURL, "xmlreadertarget?path");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    targetPath = response.getResponseBodyAsString();
  }

  private ApplicationManager deployApplication(Map<String, String> sourceProperties, String applicationName,
                                               String outputDatasetName) throws Exception {
    ETLStage source = new ETLStage("XMLReader", new ETLPlugin("XMLReader", BatchSource.PLUGIN_TYPE, sourceProperties,
                                                              null));
    ETLStage transform =
      new ETLStage("XMLProjectionTransform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                           ImmutableMap.of("convert", "offset:string", "schema",
                                                                           TRANSFORM_SCHEMA.toString()), null));
    ETLStage sink =
      new ETLStage("XMLTableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "offset",
                                                   Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = NamespaceId.DEFAULT.app(applicationName);
    return deployApplication(appId.toId(), appRequest);
  }

  private void startWorkFlow(ApplicationManager appManager) throws TimeoutException, InterruptedException {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  @Test
  /**
   * This test case is for
   * 1. Not to process already processed xml file.
   * 2. Read xml with valid node path
   * 3. Archiv xml to the target location provided
   * 4. Delete data from file track table which is after expiry period.
   */
  public void testPreProcessingNotRequired() throws Exception {
    String xmlTrackingTable = "XMLNoPreProcessingReqTrackingTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderNoPreProcessingRequiredTest")
      .put("path", sourcePath)
      .put("targetFolder", targetPath)
      .put("nodePath", "/catalog/book/price")
      .put("reprocessingRequired", "No")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "archive")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-no-preprocessing-test";
    ApplicationManager appManager = deployApplication(sourceProperties, "XMLPreprocessingNotRequiredTest",
                                                      outputDatasetName);

    //Set pre-processed file data
    DataSetManager<KeyValueTable> trackingTable = getKVTableDataset(xmlTrackingTable);
    KeyValueTable keyValueTable = trackingTable.get();

    //Set expired record, 30 days old
    String expiredPreprocessedFilePath = sourcePath + "catalog.xml";
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -30);
    long expiryPreprocessedTime = cal.getTime().getTime();
    keyValueTable.write(Bytes.toBytes(expiredPreprocessedFilePath), Bytes.toBytes(expiryPreprocessedTime));

    String preprocessedFilePath = sourcePath + "catalogProcessedFile.xml";
    long preProcessedTime = new Date().getTime();
    keyValueTable.write(Bytes.toBytes(preprocessedFilePath), Bytes.toBytes(preProcessedTime));
    trackingTable.flush();

    //Manually trigger the pipeline
    startWorkFlow(appManager);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row firstRow = outputTable.get(Bytes.toBytes("22"));
    Assert.assertEquals("<price><base>44.95</base><tax><surcharge>10.00</surcharge><excise>10.00</excise></tax></price>"
      , firstRow.getString("record"));
    Row lastRow = outputTable.get(Bytes.toBytes("128"));
    Assert.assertEquals("<price><base>49.95</base><tax><surcharge>21.00</surcharge><excise>21.00</excise></tax></price>"
      , lastRow.getString("record"));

    //File must get deleted from source location after archiving it.
    URL url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertFalse(Boolean.valueOf(response.getResponseBodyAsString()));

    //File must get archived to target location.
    url = new URL(serviceURL, "fileExist/xmlreadertarget?path=catalog.xml.zip");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    //Processing time of the prepocessed file with expired processed time must change..
    byte[] expiredFileprocessedTime = keyValueTable.read(expiredPreprocessedFilePath);
    Assert.assertNotEquals(expiryPreprocessedTime, Bytes.toLong(expiredFileprocessedTime));

    //Processing time of the prepocessed file must not change.
    byte[] preprocessedFileTime = keyValueTable.read(preprocessedFilePath);
    Assert.assertEquals(preProcessedTime, Bytes.toLong(preprocessedFileTime));
  }

  @Test
  /**
   * This test case is for
   * 1. Pre-processing required - process already processed xml file.
   * 2. Read xml with valid node path.
   * 3. No File action - file must not get deleted, moved or archived.
   */
  public void testPreProcessingRequired() throws Exception {
    String xmlTrackingTable = "XMLPreProcessingReqTrackingTable";
    String preprocessedFilePath = sourcePath + "catalog.xml";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderPreProcessingRequiredTest")
      .put("path", preprocessedFilePath)
      .put("targetFolder", targetPath)
      .put("nodePath", "/catalog/book/price")
      .put("reprocessingRequired", "Yes")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "none")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-preprocessing-test";
    ApplicationManager appManager = deployApplication(sourceProperties, "XMLPreprocessingRequiredTest",
                                                      outputDatasetName);

    //Set pre-processed file data
    DataSetManager<KeyValueTable> trackingTable = getKVTableDataset(xmlTrackingTable);
    KeyValueTable keyValueTable = trackingTable.get();

    long preProcessedTime = new Date().getTime();
    keyValueTable.write(Bytes.toBytes(preprocessedFilePath), Bytes.toBytes(preProcessedTime));
    trackingTable.flush();

    //Manually trigger the pipeline
    startWorkFlow(appManager);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row row = outputTable.get(Bytes.toBytes("128"));
    Assert.assertEquals("<price><base>49.95</base><tax><surcharge>21.00</surcharge><excise>21.00</excise></tax></price>"
      , row.getString("record"));

    //File not get deleted, moved or archived.
    URL url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    //Processing time of the prepocessed file must change.
    byte[] preprocessedFileTime = keyValueTable.read(preprocessedFilePath);
    Assert.assertNotEquals(preProcessedTime, Bytes.toLong(preprocessedFileTime));
  }

  @Test
  /**
   * This test case is for
   * 1. Not to process already processed xml file.
   * 2. Read xml with invalid node path.
   * 3. Move xml to the target location provided
   * 4. No data get Deleted from file track table which is before expiry period.
   */
  public void testInvalidNodePathWithMoveAction() throws Exception {
    String xmlTrackingTable = "XMLInvalidNodePathTrackingTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderInvalidNodePathTest")
      .put("path", sourcePath)
      .put("targetFolder", targetPath)
      .put("nodePath", "/catalog/book/prices") //invalid path, price changed to prices
      .put("reprocessingRequired", "No")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "move")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-invalid-nodepath-test";
    ApplicationManager appManager = deployApplication(sourceProperties, "XMLInvalidNodePathTest",
                                                      outputDatasetName);

    //Set pre-processed file data
    DataSetManager<KeyValueTable> trackingTable = getKVTableDataset(xmlTrackingTable);
    KeyValueTable keyValueTable = trackingTable.get();

    //Set expired record, 20 days old
    String preprocessedFilePath = sourcePath + "catalogProcessedFile.xml";
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -20);
    long preprocessedTime = cal.getTime().getTime();
    keyValueTable.write(Bytes.toBytes(preprocessedFilePath), Bytes.toBytes(preprocessedTime));
    trackingTable.flush();

    //Manually trigger the pipeline
    startWorkFlow(appManager);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row row1 = outputTable.get(Bytes.toBytes("22"));
    Assert.assertNull(row1.getString("record"));
    Row row2 = outputTable.get(Bytes.toBytes("128"));
    Assert.assertNull(row2.getString("record"));

    //File must get deleted from source location after archiving it.
    URL url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertFalse(Boolean.valueOf(response.getResponseBodyAsString()));

    //File must get moved to target location.
    url = new URL(serviceURL, "fileExist/xmlreadertarget?path=catalog.xml");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    //20 days old processed file data not get deleted and no processing time changed.
    url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalogProcessedFile.xml");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    byte[] preprocessedFileTime = keyValueTable.read(preprocessedFilePath);
    Assert.assertEquals(preprocessedTime, Bytes.toLong(preprocessedFileTime));
  }

  @Test
  /**
   * This test case is for
   * 1. Read xml file matching to pattern provided.
   * 2. Delete xml file after processing.
   */
  public void testPatternWithDeleteAction() throws Exception {
    String xmlTrackingTable = "XMLPatternTrackingTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderPatternTest")
      .put("path", sourcePath)
      .put("targetFolder", targetPath)
      .put("pattern", "log.xml$") //file name ends with log.xml
      .put("nodePath", "/catalog/book/price")
      .put("reprocessingRequired", "No")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "delete")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-pattern-test";
    ApplicationManager appManager = deployApplication(sourceProperties, "XMLPatternTest",
                                                      outputDatasetName);

    //Manually trigger the pipeline
    startWorkFlow(appManager);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row firstRow = outputTable.get(Bytes.toBytes("22"));
    Assert.assertEquals("<price><base>44.95</base><tax><surcharge>10.00</surcharge><excise>10.00</excise></tax></price>"
      , firstRow.getString("record"));
    Row lastRow = outputTable.get(Bytes.toBytes("128"));
    Assert.assertEquals("<price><base>49.95</base><tax><surcharge>21.00</surcharge><excise>21.00</excise></tax></price>"
      , lastRow.getString("record"));

    //File must get deleted from source location after archiving it.
    URL url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertFalse(Boolean.valueOf(response.getResponseBodyAsString()));

    //File not matching pattern should not get processed.
    DataSetManager<KeyValueTable> trackingTable = getKVTableDataset(xmlTrackingTable);
    KeyValueTable keyValueTable = trackingTable.get();
    byte[] processedTime = keyValueTable.read(sourcePath + "catalogProcessedFile.xml");
    Assert.assertNull(processedTime);
    //File not matching pattern should not get deleted.
    url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalogProcessedFile.xml");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));
  }

  @Test
  public void testInvalidPatternWithDeleteAction() throws Exception {
    String xmlTrackingTable = "XMLInvalidPatternTrackingTable";
    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderInvalidPatternTest")
      .put("path", sourcePath)
      .put("targetFolder", targetPath)
      .put("pattern", "^small") //file name start with small, invalid pattern
      .put("nodePath", "/catalog/book/price")
      .put("reprocessingRequired", "No")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "delete")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    String outputDatasetName = "output-batchsink-invalid-pattern-test";
    ApplicationManager appManager = deployApplication(sourceProperties, "XMLInvalidPatternTest",
                                                      outputDatasetName);

    //Manually trigger the pipeline
    startWorkFlow(appManager);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    //No file read, hence record must not present.
    Row row = outputTable.get(Bytes.toBytes("22"));
    Assert.assertNull(row.getString("record"));

    //File must not get deleted from source location.
    URL url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalogProcessedFile.xml");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertTrue(Boolean.valueOf(response.getResponseBodyAsString()));

    //No file get processed.
    DataSetManager<KeyValueTable> trackingTable = getKVTableDataset(xmlTrackingTable);
    KeyValueTable keyValueTable = trackingTable.get();
    byte[] catalogProcessTime = keyValueTable.read(sourcePath + "catalog.xml");
    Assert.assertNull(catalogProcessTime);
    byte[] catalogProcessedFileProcessTime = keyValueTable.read(sourcePath + "catalogProcessedFile.xml");
    Assert.assertNull(catalogProcessedFileProcessTime);
  }
}
