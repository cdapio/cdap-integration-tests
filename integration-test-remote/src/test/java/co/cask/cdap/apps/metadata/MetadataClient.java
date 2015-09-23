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
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Metadata.
 */
public class MetadataClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetadataClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MetadataClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Returns search results for metadata.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param target the target type
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public Set<MetadataSearchResultRecord> searchMetadata(Id.Namespace namespace, String query,
                                                        MetadataSearchTargetType target)
    throws IOException, UnauthorizedException {

    String path = String.format("metadata/search?query=%s&target=%s", query, target.getInternalName());
    URL searchURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.get(searchURL).build(),
                                               config.getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_SEARCH_RESULT_TYPE);
  }


  /**
   * @param appId the app for which to retrieve metadata
   * @return The metadata for the application.
   */
  public Set<MetadataRecord> getMetadata(Id.Application appId) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/metadata", appId.getId());
    return getMetadata(appId.getNamespace(), path);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata
   * @return The metadata for the dataset.
   */
  public Set<MetadataRecord> getMetadata(Id.DatasetInstance datasetInstance) throws IOException, UnauthorizedException {
    String path = String.format("datasets/%s/metadata", datasetInstance.getId());
    return getMetadata(datasetInstance.getNamespace(), path);
  }

  /**
   * @param streamId the stream for which to retrieve metadata
   * @return The metadata for the stream.
   */
  public Set<MetadataRecord> getMetadata(Id.Stream streamId) throws IOException, UnauthorizedException {
    String path = String.format("streams/%s/metadata", streamId.getId());
    return getMetadata(streamId.getNamespace(), path);
  }

  /**
   * @param programId the program for which to retrieve metadata
   * @return The metadata for the program.
   */
  public Set<MetadataRecord> getMetadata(Id.Program programId) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/%s/%s/metadata",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    return getMetadata(programId.getNamespace(), path);
  }

  /**
   * @param runId the run for which to retrieve metadata
   * @return The metadata for the run.
   */
  public Set<MetadataRecord> getMetadata(Id.Run runId) throws IOException, UnauthorizedException {
    Id.Program programId = runId.getProgram();
    String path = String.format("apps/%s/%s/%s/runs/%s/metadata",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId(),
                                runId.getId());
    return getMetadata(runId.getNamespace(), path);
  }

  private Set<MetadataRecord> getMetadata(Id.Namespace namespace,
                                          String path) throws IOException, UnauthorizedException {
    URL metadataURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.get(metadataURL).build(), config.getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  /**
   * Adds tags to an application.
   *
   * @param appId app to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Application appId, List<String> tags) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/metadata/tags", appId.getId());
    addTags(appId.getNamespace(), path, tags);
  }

  /**
   * Adds tags to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.DatasetInstance datasetInstance,
                      List<String> tags) throws IOException, UnauthorizedException {
    String path = String.format("datasets/%s/metadata/tags", datasetInstance.getId());
    addTags(datasetInstance.getNamespace(), path, tags);
  }

  /**
   * Adds tags to a stream.
   *
   * @param streamId stream to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Stream streamId, List<String> tags) throws IOException, UnauthorizedException {
    String path = String.format("streams/%s/metadata/tags", streamId.getId());
    addTags(streamId.getNamespace(), path, tags);
  }

  /**
   * Adds tags to a program.
   *
   * @param programId program to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Program programId, List<String> tags) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/%s/%s/metadata/tags",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    addTags(programId.getNamespace(), path, tags);
  }

  private void addTags(Id.Namespace namespace, String path,
                       List<String> tags) throws IOException, UnauthorizedException {
    URL tagURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.post(tagURL).withBody(GSON.toJson(tags)).build(),
                                               config.getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }

  /**
   * Adds properties to an application.
   *
   * @param appId app to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Application appId,
                            Map<String, String> properties) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/metadata/properties", appId.getId());
    addProperties(appId.getNamespace(), path, properties);
  }

  /**
   * Adds properties to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.DatasetInstance datasetInstance,
                            Map<String, String> properties) throws IOException, UnauthorizedException {
    String path = String.format("datasets/%s/metadata/properties", datasetInstance.getId());
    addProperties(datasetInstance.getNamespace(), path, properties);
  }

  /**
   * Adds properties to a stream.
   *
   * @param streamId stream to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Stream streamId,
                            Map<String, String> properties) throws IOException, UnauthorizedException {
    String path = String.format("streams/%s/metadata/properties", streamId.getId());
    addProperties(streamId.getNamespace(), path, properties);
  }

  /**
   * Adds properties to a program.
   *
   * @param programId program to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Program programId,
                            Map<String, String> properties) throws IOException, UnauthorizedException {
    String path = String.format("apps/%s/%s/%s/metadata/properties",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    addProperties(programId.getNamespace(), path, properties);
  }

  private void addProperties(Id.Namespace namespace, String path,
                             Map<String, String> properties) throws IOException, UnauthorizedException {
    URL tagURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.post(tagURL).withBody(GSON.toJson(properties)).build(),
                                               config.getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }
}
