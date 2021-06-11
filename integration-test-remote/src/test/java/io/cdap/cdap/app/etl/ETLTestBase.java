/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.etl;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.InvalidArtifactRangeException;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.hub.PluginAttributes;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.proto.ConfigEntry;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.ArtifactRanges;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An abstract class for writing etl integration tests. Tests for etl should extend this class.
 */
public abstract class ETLTestBase extends AudiTestBase {
  private static final Gson GSON = new Gson();
  protected static final String SOURCE_DATASET = "sourceDataset";
  protected static final Schema DATASET_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  protected ApplicationClient appClient;
  protected DatasetClient datasetClient;
  protected ArtifactClient artifactClient;
  protected String version;

  @Before
  public void setup() throws InterruptedException, ExecutionException, TimeoutException {
    appClient = getApplicationClient();
    datasetClient = getDatasetClient();
    artifactClient = new ArtifactClient(getClientConfig(), getRestClient());

    version = getVersion();
    final ArtifactId datapipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
    final ArtifactId datastreamsId = TEST_NAMESPACE.artifact("cdap-data-streams", version);

    // wait until we see extensions for cdap-data-pipeline and cdap-data-streams
    Tasks.waitFor(true, () -> {
      try {
        // cdap-data-pipeline and cdap-data-streams are parent artifacts
        List<PluginSummary> plugins =
          artifactClient.getPluginSummaries(datapipelineId, BatchAggregator.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        if (plugins.stream().noneMatch(pluginSummary -> "GroupByAggregate".equals(pluginSummary.getName()))) {
          return false;
        }

        plugins = artifactClient.getPluginSummaries(datapipelineId, BatchSink.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        if (plugins.stream().noneMatch(pluginSummary -> "File".equals(pluginSummary.getName()))) {
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
  protected AppRequest<ETLBatchConfig> getBatchAppRequestV2(io.cdap.cdap.etl.proto.v2.ETLBatchConfig config) {
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

  protected void installPluginFromHub(String packageName, String pluginName, String version)
    throws IOException, BadRequestException, ArtifactRangeNotFoundException {
    Map<String, ConfigEntry> cdapConfig = getMetaClient().getCDAPConfig();

    String hubURL = cdapConfig.get("market.base.url").getValue();
    URL pluginJsonURL = new URL(String.format("%s/packages/%s/%s/%s-%s.json",
                                              hubURL, packageName, version, pluginName, version));
    // Avoid passing in an authentication token, which results in an auth failure when making requests to
    // the CDAP Hub. The CDAP Hub is available without any authentication.
    HttpResponse response = getRestClient().execute(HttpRequest.get(pluginJsonURL).build());
    Assert.assertEquals(200, response.getResponseCode());

    // get the artifact 'parents' from the plugin json
    PluginAttributes pluginAttributes = GSON.fromJson(response.getResponseBodyAsString(), PluginAttributes.class);
    List<String> parentStrings = pluginAttributes.getParents();
    Set<ArtifactRange> parents = parentStrings.stream()
      .map(p -> {
        try {
          return ArtifactRanges.parseArtifactRange(p);
        } catch (InvalidArtifactRangeException e) {
          throw new RuntimeException(e);
        }
      })
      .collect(Collectors.toSet());

    URL pluginJarURL = new URL(String.format("%s/packages/%s/%s/%s-%s.jar",
                                             hubURL, packageName, version, pluginName, version));

    try {
      ArtifactId artifactId = TEST_NAMESPACE.artifact(pluginName, version);
      artifactClient.add(artifactId, parents, pluginJarURL::openStream);
    } catch (ArtifactAlreadyExistsException e) {
      // ignore since it already exists
    }
  }

  protected void ingestData() throws Exception {
    // write input data
    DataSetManager<Table> datasetManager = getTableDataset(SOURCE_DATASET);
    Table table = datasetManager.get();
    // AAPL|10|500.32 with dummy timestamp
    putValues(table, 1, 234, "AAPL", 10, 500.32);
    datasetManager.flush();
  }

  private void putValues(Table table, int index, long ts, String ticker, int num, double price) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("ts", ts);
    put.add("ticker", ticker);
    put.add("num", num);
    put.add("price", price);
    table.put(put);
  }
}
