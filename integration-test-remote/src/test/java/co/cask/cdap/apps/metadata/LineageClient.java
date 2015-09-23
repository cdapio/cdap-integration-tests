/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps.metadata;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Lineage.
 */
public class LineageClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public LineageClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public LineageClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Retrieves Lineage for a given dataset.
   *
   * @param datasetInstance the dataset for which to retrieve lineage
   * @param startTime start time for the query, in seconds
   * @param endTime end time for the query, in seconds
   * @return {@link LineageRecord} for the specified dataset.
   */
  public LineageRecord getDatasetLineage(Id.DatasetInstance datasetInstance,
                                         long startTime, long endTime) throws IOException, UnauthorizedException {
    String path = String.format("datasets/%s/lineage?start=%s&end=%s", datasetInstance.getId(), startTime, endTime);
    return getLineage(datasetInstance.getNamespace(), path);
  }

  /**
   * Retrieves Lineage for a given stream.
   *
   * @param streamId the stream for which to retrieve lineage
   * @param startTime start time for the query, in seconds
   * @param endTime end time for the query, in seconds
   * @return {@link LineageRecord} for the specified stream.
   */
  public LineageRecord getStreamLineage(Id.Stream streamId,
                                        long startTime, long endTime) throws IOException, UnauthorizedException {
    String path = String.format("streams/%s/lineage?start=%s&end=%s", streamId.getId(), startTime, endTime);
    return getLineage(streamId.getNamespace(), path);
  }

  private LineageRecord getLineage(Id.Namespace namespace, String path) throws IOException, UnauthorizedException {
    URL lineageURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.get(lineageURL).build(),
                                               config.getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
  }
}
