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

package io.cdap.cdap.app.etl.batch;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.apps.fileset.FileSetExample;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.cdap.test.suite.category.SDKIncompatible;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for HiveImport plugin.
 */
@Category({
  // Do not run the tests on SDK because there no Hive server available for the HivePlugin to connect to.
  SDKIncompatible.class
})
public class HivePluginTest extends ETLTestBase {

  private static final List<String> DATA_LIST =
    // already sorted, for list comparison at the end of the test
    Lists.newArrayList("Hello World", "My Hello Hello World", "World Hello");
  private static final String DATA_UPLOAD = Joiner.on("\n").join(DATA_LIST);

  @Test
  public void testHivePlugins() throws Exception {
    installPluginFromMarket("hydrator-plugin-hive", "hive-plugins", "1.8.0-1.1.0");

    ApplicationManager applicationManager = deployApplication(FileSetExample.class);
    ServiceManager fileSetService = applicationManager.getServiceManager("FileSetService").start();
    fileSetService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceURL = fileSetService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    DatasetProperties datasetProperties = FileSetProperties.builder()
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema("`data` STRING")
      .setFilePermissions("777") // 'hive' user needs to be able to read/write in the FileSet, when using HivePlugins
      .build();

    // create two hive-backed FileSets
    String inputDataset = "inputDataset";
    String outputDataset = "outputDataset";
    getRestClient().execute(HttpRequest.post(new URL(serviceURL, inputDataset + "/create"))
                              .withBody(new Gson().toJson(datasetProperties)).build(),
                            getClientConfig().getAccessToken());
    getRestClient().execute(HttpRequest.post(new URL(serviceURL, outputDataset + "/create"))
                              .withBody(new Gson().toJson(datasetProperties)).build(),
                            getClientConfig().getAccessToken());


    // upload some data to one of the FileSets
    URL url = new URL(serviceURL, inputDataset + "?path=myFile.txt");
    HttpResponse response = getRestClient().execute(HttpRequest.put(url).withBody(DATA_UPLOAD).build(),
                                       getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(DATA_UPLOAD, response.getResponseBodyAsString());

    // get the FileSet's full path from explore REST APIs
    url = getClientConfig().getConnectionConfig().resolveNamespacedURI(
      TEST_NAMESPACE, "v3", "data/explore/tables/dataset_" + inputDataset + "/info").toURL();
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    String location =
      new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonObject().get("location").getAsString();

    Map<String, String> properties =
      ImmutableMap.of("connectionString", getHiveConnectionString(),
                      "statement", "LOAD DATA INPATH '" + location + "' INTO TABLE dataset_" + outputDataset);

    // use a pipeline that imports data from one FileSet to the other, using the HiveImport plugin
    ETLStage hiveImport = new ETLStage("HiveImportStage",
                                       new ETLPlugin("HiveImport", Action.PLUGIN_TYPE, properties, null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(hiveImport)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("HiveImportApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME).start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // query the outputDataset to ensure that the data was written by the pipeline
    QueryClient queryClient = new QueryClient(getClientConfig());
    String exploreQuery = "SELECT * FROM dataset_" + outputDataset;

    // Reduce wait time by submitting both the queries
    ListenableFuture<ExploreExecutionResult> cleanRecordsExecute =
      queryClient.execute(TEST_NAMESPACE, exploreQuery);
    ExploreExecutionResult cleanRecordsResult = cleanRecordsExecute.get();

    List<String> records = new ArrayList<>();
    while (cleanRecordsResult.hasNext()) {
      records.add((String) cleanRecordsResult.next().getColumns().get(0));
    }

    Collections.sort(records);
    Assert.assertEquals(DATA_LIST, records);
  }

  // construct a hive connection string, based upon cdap configs
  private String getHiveConnectionString() throws IOException, UnauthenticatedException {
    String jdbcURL = getMetaClient().getCDAPConfig().get("hive.server2.jdbc.url").getValue();
    // remove 'principal=hive/_HOST@REALM.NET'
    jdbcURL = jdbcURL.replaceFirst("principal=.*?(;|$)", "");

    // put the hive database to use in the URL
    int firstSemiColon = jdbcURL.indexOf(";");
    if (firstSemiColon == -1) {
      firstSemiColon = jdbcURL.length();
    }
    int firstSlashIndex = jdbcURL.substring(0, firstSemiColon).lastIndexOf("/");
    if (firstSlashIndex == -1) {
      throw new IllegalStateException("JDBC URL must have at least one slash: " + jdbcURL);
    }
    jdbcURL = jdbcURL.substring(0, firstSlashIndex + 1) + getHiveDatabase() + jdbcURL.substring(firstSemiColon);

    return jdbcURL + ";auth=delegationToken;user=cdap";
  }

  private String getHiveDatabase() {
    if (NamespaceId.DEFAULT.equals(TEST_NAMESPACE)) {
      return NamespaceId.DEFAULT.getNamespace();
    }
    return "cdap_" + TEST_NAMESPACE.getNamespace();
  }
}
