/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * An abstract class for writing etl integration tests. Tests for etl should extend this class.
 */
public abstract class ETLTestBase extends AudiTestBase {

  protected static final String DUMMY_STREAM_EVENT = "AAPL|10|500.32";
  protected static final Schema DUMMY_STREAM_EVENT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
  private static final String CASK_MARKET_URI = System.getProperty("cask.market.uri", "http://market.cask.co/v2");

  protected ETLStageProvider etlStageProvider;
  protected StreamClient streamClient;
  protected ApplicationClient appClient;
  protected DatasetClient datasetClient;
  protected ArtifactClient artifactClient;
  protected String version;

  @Before
  public void setup() throws InterruptedException, ExecutionException, TimeoutException {
    appClient = getApplicationClient();
    datasetClient = getDatasetClient();
    etlStageProvider = new ETLStageProvider();
    streamClient = getStreamClient();
    artifactClient = new ArtifactClient(getClientConfig(), getRestClient());

    version = getVersion();
    final ArtifactId datapipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
    final ArtifactId datastreamsId = TEST_NAMESPACE.artifact("cdap-data-streams", version);

    // wait until we see extensions for cdap-data-pipeline and cdap-data-streams
    Tasks.waitFor(true, () -> {
      try {
        List<PluginSummary> plugins =
          artifactClient.getPluginSummaries(datapipelineId, BatchAggregator.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        if (plugins.stream().noneMatch(pluginSummary -> "GroupByAggregate".equals(pluginSummary.getName()))) {
          return false;
        }

        plugins = artifactClient.getPluginSummaries(datapipelineId, BatchSink.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        if (plugins.stream().noneMatch(pluginSummary -> "HDFS".equals(pluginSummary.getName()))) {
          return false;
        }

        plugins = artifactClient.getPluginSummaries(datastreamsId, BatchAggregator.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        if (plugins.stream().noneMatch(pluginSummary -> "GroupByAggregate".equals(pluginSummary.getName()))) {
          return false;
        }

        return true;
      } catch (ArtifactNotFoundException e) {
        // happens if cdap-data-pipeline or cdap-data-streams were not added yet
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  protected AppRequest<DataStreamsConfig> getStreamingAppRequest(DataStreamsConfig config) {
    return new AppRequest<>(new ArtifactSummary("cdap-data-streams", version, ArtifactScope.SYSTEM), config);
  }

  @Nullable
  protected AppRequest getWranglerAppRequest(List<ArtifactSummary> list) {
    //arbitrary AppRequest
    AppRequest request = null;
    for (ArtifactSummary summary : list) {
      if (summary.getName().contains("wrangler-service")) {
        request = new AppRequest<>(summary);
      }
    }
    return request;
  }

  // make the above two methods use this method instead
  protected AppRequest<ETLBatchConfig> getBatchAppRequestV2(co.cask.cdap.etl.proto.v2.ETLBatchConfig config) {
    return new AppRequest<>(new ArtifactSummary("cdap-data-pipeline", version, ArtifactScope.SYSTEM), config);
  }

  private String getVersion() {
    if (version == null) {
      try {
        version = getMetaClient().getVersion().getVersion();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
    return version;
  }

  protected void installPluginFromMarket(String packageName, String pluginName, String version)
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
      String.format("v3/namespaces/%s/artifacts/%s", TEST_NAMESPACE.getNamespace(), pluginName)).toString(), "UTF-8");

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
