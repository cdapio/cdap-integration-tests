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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
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
import java.util.concurrent.TimeUnit;

public class CopybookReaderTest extends ETLTestBase {

  private URL serviceURL;

  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(UploadFile.
                                                                           FileSetService.class.getSimpleName());
    serviceManager.start();
    serviceURL = serviceManager.getServiceURL();
    URL url = new URL(serviceURL, "copybook/create");
    //POST request to create a new file set with name copybook
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    url = new URL(serviceURL, "copybook?path=DTAR020_FB.bin");
    //PUT request to upload the binary file, sent in the request body
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.put(url).
      withBody(new File("src/test/resources/DTAR020_FB.bin")).build());
  }

  @Test
  public void test() throws Exception {
    //GET location of the fileset copybook on cluster
    URL url = new URL(serviceURL, "copybook?path=DTAR020_FB.bin");
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    String cblContents = "000100*                                                                         \n" +
      "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
      "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
      "000400*                                                                         \n" +
      "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
      "000600*                                                                         \n" +
      "000700*   RECORD LENGTH IS 27.                                                  \n" +
      "000800*                                                                         \n" +
      "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
      "001000            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
      "001100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
      "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3.               \n" +
      "001300        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               \n" +
      "001400        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               \n" +
      "001500        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.";

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("key", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("dept", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("qty", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("sale", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

    ETLStage source = new ETLStage("CopybookReaderSource",
                                   new ETLPlugin("CopybookReader", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "input",
                                                                 Constants.Reference.REFERENCE_NAME,
                                                                 "CopybookReaderSourceTest",
                                                                 "binaryFilePath", response.getResponseBodyAsString(),
                                                                 "copybookContents", cblContents,
                                                                 "drop", "DTAR020-STORE-NO,DTAR020-DATE"), null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage transform =
      new ETLStage("ProjectionTransform2", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                         ImmutableMap.of("name", "classifiedTexts",
                                                                         "rename", "DTAR020-KEYCODE-NO:key," +
                                                                           "DTAR020-DEPT-NO:dept," +
                                                                           "DTAR020-QTY-SOLD:qty," +
                                                                           "DTAR020-SALE-PRICE:sale",
                                                                         "schema", schema.toString()), null));
    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, outputDatasetName,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "key",
                                                Properties.Table.PROPERTY_SCHEMA, schema.toString()), null));

    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "CopybookReaderTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(outputDatasetName);
    Table outputTable = outputManager.get();
    Row row = outputTable.get(Bytes.toBytes("63604808"));
    Assert.assertEquals(4.87, row.getDouble("sale"), 0.1);
    Assert.assertEquals(170, row.getDouble("dept"), 0.1);
  }
}
