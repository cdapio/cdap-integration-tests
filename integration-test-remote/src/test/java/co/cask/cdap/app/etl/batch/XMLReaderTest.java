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

/**
 * Test for XMLReaderBatchSource.
 */
public class XMLReaderTest extends ETLTestBase {
  private URL serviceURL;

  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(UploadFile.
                                                                           FileSetService.class.getSimpleName());
    serviceManager.start();
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
  }

  @Test
  public void test() throws Exception {
    //GET location of the fileset xmlreadersource on cluster.
    URL url = new URL(serviceURL, "xmlreadersource?path");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String sourcePath = response.getResponseBodyAsString();

    //GET location of the fileset xmlreadertarget on cluster.
    url = new URL(serviceURL, "xmlreadertarget?path");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String targetPath = response.getResponseBodyAsString();

    String xmlTrackingTable = "XMLTrackingTable";

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "XMLReaderBatchSourceTest")
      .put("path", sourcePath)
      .put("targetFolder", targetPath)
      .put("nodePath", "/catalog/book/price")
      .put("reprocessingRequired", "No")
      .put("tableName", xmlTrackingTable)
      .put("actionAfterProcess", "archive")
      .put("tableExpiryPeriod", "30")
      .put("temporaryFolder", "/tmp")
      .build();

    ETLStage source = new ETLStage("XMLReader", new ETLPlugin("XMLReader", BatchSource.PLUGIN_TYPE, sourceProperties,
                                                              null));
    Schema transformSchema = Schema.recordOf(
      "xmlTransform",
      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("record", Schema.of(Schema.Type.STRING)));


    ETLStage transform =
      new ETLStage("XMLProjectionTransform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                           ImmutableMap.of("convert", "offset:string", "schema",
                                                                           transformSchema.toString()), null));

    Schema sinkSchema = Schema.recordOf(
      "xmlSink",
      Schema.Field.of("offset", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("record", Schema.of(Schema.Type.STRING)));

    String outputDatasetName = "output-batchsink-test";
    ETLStage sink =
      new ETLStage("XMLTableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "offset",
                                                   Properties.Table.PROPERTY_SCHEMA, sinkSchema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLReaderTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

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
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row firstRow = outputTable.get(Bytes.toBytes("22"));
    Assert.assertEquals("<price><base>44.95</base><tax><surcharge>10.00</surcharge><excise>10.00</excise></tax></price>"
      , firstRow.getString("record"));
    Row lastRow = outputTable.get(Bytes.toBytes("128"));
    Assert.assertEquals("<price><base>49.95</base><tax><surcharge>21.00</surcharge><excise>21.00</excise></tax></price>"
      , lastRow.getString("record"));

    //File must get deleted from source location after archiving it.
    url = new URL(serviceURL, "fileExist/xmlreadersource?path=catalog.xml");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
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
}
