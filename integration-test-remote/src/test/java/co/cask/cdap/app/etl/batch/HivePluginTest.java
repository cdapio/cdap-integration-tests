/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.examples.fileset.FileSetExample;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for HiveImport plugin.
 */
public class HivePluginTest extends ETLTestBase {

  private static final String CASK_MARKET_URI = System.getProperty("cask.market.uri", "http://market.cask.co/v2");
  private static final List<String> DATA_LIST =
    // already sorted, for list comparison at the end of the test
    Lists.newArrayList("Hello World", "My Hello Hello World", "World Hello");
  private static final String DATA_UPLOAD = Joiner.on("\n").join(DATA_LIST);

  @Test
  public void testHivePlugins() throws Exception {
    setupHivePlugins("hydrator-plugin-hive", "hive-plugins", "1.7.2-1.1.0");

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
  private String getHiveConnectionString() throws IOException, UnauthenticatedException, URISyntaxException {
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

  private void setupHivePlugins(String packageName, String pluginName, String version)
    throws IOException, UnauthenticatedException {
    URL pluginJsonURL = new URL(String.format("%s/packages/%s/%s/%s-%s.json",
                                              CASK_MARKET_URI, packageName, version, pluginName, version));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pluginJsonURL, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    // get the artifact 'parents' from the plugin json
    JsonObject pluginJson = new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonObject();
    JsonArray parents = pluginJson.get("parents").getAsJsonArray();
    List<String> parentStrings = new ArrayList<>();
    for (JsonElement parent : parents) {
      parentStrings.add(parent.getAsString());
    }

    // leverage a UI endpoint to upload the plugins from market
    String source = URLEncoder.encode(
      String.format("%s/packages/%s/%s/%s-%s.jar",
                    CASK_MARKET_URI, packageName, version, pluginName, version), "UTF-8");
    String target = URLEncoder.encode(getClientConfig().getConnectionConfig().resolveURI(
      String.format("v3/namespaces/%s/artifacts/hive-plugins", TEST_NAMESPACE.getNamespace())).toString(), "UTF-8");

    Map<String, ConfigEntry> cdapConfig = getMetaClient().getCDAPConfig();
    ConnectionConfig connConfig = getClientConfig().getConnectionConfig();
    String uiPort = connConfig.isSSLEnabled() ?
      cdapConfig.get("dashboard.ssl.bind.port").getValue() : cdapConfig.get("dashboard.bind.port").getValue();
    String url =
      String.format("%s://%s:%s/forwardMarketToCdap?source=%s&target=%s",
                    connConfig.isSSLEnabled() ? "https" : "http",
                    connConfig.getHostname(), // just assume that UI is colocated with Router
                    uiPort,
                    source, target);

    Map<String, String> headers =
      ImmutableMap.of("Artifact-Extends", Joiner.on("/").join(parentStrings),
                      "Artifact-Version", version);
    response = getRestClient().execute(HttpMethod.GET, new URL(url), headers, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }
}
