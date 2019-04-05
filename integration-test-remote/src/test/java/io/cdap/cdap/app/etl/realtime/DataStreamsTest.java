/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.realtime;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.app.etl.batch.UploadFile;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
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
import io.cdap.cdap.test.suite.category.CDH54Incompatible;
import io.cdap.cdap.test.suite.category.HDP22Incompatible;
import io.cdap.cdap.test.suite.category.RequiresSpark;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for DataStreams app.
 */
public class DataStreamsTest extends ETLTestBase {

  @Category({
    RequiresSpark.class,
    // (CDAP-10143) Mark HDP 2.2 and CDH 5.4 incompatible at least until we resolve this JIRA.
    HDP22Incompatible.class,
    CDH54Incompatible.class,
  })

  @Test
  public void testDataStreams() throws Exception {

    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL url = new URL(serviceURL, "testFileSet/create");
    //POST request to create a new file set with name testFileSet.
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    URL pathServiceUrl = new URL(serviceURL, "testFileSet?path");
    AccessToken accessToken = getClientConfig().getAccessToken();
    HttpResponse sourceResponse = getRestClient().execute(HttpMethod.GET, pathServiceUrl, accessToken);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, sourceResponse.getResponseCode());
    String sourcePath = sourceResponse.getResponseBodyAsString();

    final String tableName = "rappers";

    Schema fileSchema = Schema.recordOf("etlSchemaBody",
                                        Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("fname", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("lname", Schema.of(Schema.Type.STRING)));
    ETLStage source =
      new ETLStage("File",
                   new ETLPlugin("File", StreamingSource.PLUGIN_TYPE,
                                 ImmutableMap.of("schema", fileSchema.toString(),
                                                 "format", "csv",
                                                 "referenceName", "File",
                                                 "path", sourcePath,
                                                 "ignoreThreshold", "600"), null));

    Schema sinkSchema = Schema.recordOf("etlSchemaBody",
                                        Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("fname", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("lname", Schema.of(Schema.Type.STRING)));
    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table",
                                              BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                "schema", sinkSchema.toString(),
                                                "schema.row.field", "id",
                                                "name", tableName), null));

    DataStreamsConfig config = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("30s")
      // stop gracefully to false so it shuts down on time
      .setStopGracefully(false)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("DataStreams-Test");
    AppRequest appRequest = getStreamingAppRequest(config);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    SparkManager sparkManager = appManager.getSparkManager("DataStreamsSparkStreaming");
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    url = new URL(serviceURL, "testFileSet?path=test1.csv");
    //PUT request to upload the test1.csv file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/test1.csv"))
                              .build(), getClientConfig().getAccessToken());

    url = new URL(serviceURL, "testFileSet?path=test2.csv");
    //PUT request to upload the test2.csv file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/test2.csv"))
                              .build(), getClientConfig().getAccessToken());

    DataSetManager<Table> tableManager = getTableDataset(tableName);
    Table table = tableManager.get();

    verifyOutput(table, "1", "Kodak", "Black");
    verifyOutput(table, "2", "Marshall", "Mathers");

    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private void verifyOutput(final Table table, final String id, String firstName, String lastName)
    throws InterruptedException, ExecutionException, TimeoutException {

    Tasks.waitFor(true, () -> {
      Row row = table.get(Bytes.toBytes(id));
      return row.getString("fname") != null;
    }, 5, TimeUnit.MINUTES, 5, TimeUnit.SECONDS);

    Row row = table.get(Bytes.toBytes(id));
    Assert.assertEquals(firstName, row.getString("fname"));
    Assert.assertEquals(lastName, row.getString("lname"));
  }
}
