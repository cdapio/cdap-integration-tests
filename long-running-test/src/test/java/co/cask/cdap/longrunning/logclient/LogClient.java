//package co.cask.cdap.longrunning.logclient;
//
///*
// * Copyright © 2015-2016 Cask Data, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//
//import co.cask.cdap.client.config.ClientConfig;
//import co.cask.cdap.client.util.RESTClient;
//import co.cask.cdap.common.BadRequestException;
//import co.cask.cdap.common.NotFoundException;
//import co.cask.cdap.common.UnauthenticatedException;
//import co.cask.cdap.proto.Id;
//import co.cask.cdap.proto.codec.NamespacedIdCodec;
//import co.cask.cdap.proto.metadata.MetadataRecord;
//import co.cask.cdap.proto.metadata.MetadataScope;
//import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
//import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
//import co.cask.common.http.HttpMethod;
//import co.cask.common.http.HttpRequest;
//import co.cask.common.http.HttpResponse;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.reflect.TypeToken;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//
//import java.io.IOException;
//import java.lang.reflect.Type;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.util.Map;
//import java.util.Set;
//import javax.annotation.Nullable;
//import javax.inject.Inject;
//
///**
// * Provides ways to interact with CDAP Metadata.
// */
//public class LogClient {
//
//  private static final Gson GSON = new GsonBuilder()
//    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();
//  private final RESTClient restClient;
//  private final ClientConfig config;
//
//  @Inject
//  public LogClient(ClientConfig config, RESTClient restClient) {
//    this.config = config;
//    this.restClient = restClient;
//  }
//
//  public LogClient(ClientConfig config) {
//    this(config, new RESTClient(config));
//  }
//
//  /**
//   * @param id the entity for which to retrieve metadata across {@link MetadataScope#SYSTEM} and
//   * {@link MetadataScope#USER}
//   * @return The metadata for the entity.
//   */
//  public Set<MetadataRecord> getLog(Id id)
//    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException {
//    return getMetadata(id, null);
//  }
//
//  /**
//   * @param id the entity for which to retrieve metadata
//   * @param scope the {@link MetadataScope} to retrieve the metadata from. If null, this method retrieves
//   *              metadata from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
//   * @return The metadata for the entity.
//   */
//  public Set<MetadataRecord> getMetadata(Id id, @Nullable MetadataScope scope)
//    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException {
//
//    if (id instanceof Id.Application) {
//      return getMetadata((Id.Application) id, scope);
//    } else if (id instanceof Id.Artifact) {
//      return getMetadata((Id.Artifact) id, scope);
//    } else if (id instanceof Id.DatasetInstance) {
//      return getMetadata((Id.DatasetInstance) id, scope);
//    } else if (id instanceof Id.Stream) {
//      return getMetadata((Id.Stream) id, scope);
//    } else if (id instanceof Id.Stream.View) {
//      return getMetadata((Id.Stream.View) id, scope);
//    } else if (id instanceof Id.Program) {
//      return getMetadata((Id.Program) id, scope);
//    } else if (id instanceof Id.Run) {
//      return getMetadata((Id.Run) id);
//    }
//
//    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
//  }
//
//
//  /**
//   * @param appId the app for which to retrieve metadata
//   * @return The metadata for the application.
//   */
//  public Set<MetadataRecord> getMetadata(Id.Application appId, @Nullable MetadataScope scope)
//    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
//    return doGetMetadata(appId, constructPath(appId), scope);
//  }
//
//  private Set<MetadataRecord> doGetMetadata(Id.NamespacedId namespacedId, String entityPath,
//                                            @Nullable MetadataScope scope)
//    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
//    String path = String.format("%s/metadata", entityPath);
//    path = scope == null ? path : String.format("%s?scope=%s", path, scope);
//    HttpResponse response = makeRequestLog(namespacedId, path, HttpMethod.GET);
//    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
//  }
//
//
//  // makes a request and throws BadRequestException or NotFoundException, as appropriate
//  private HttpResponse makeRequestLog(Id.NamespacedId namespacedId, String path,
//                                   HttpMethod httpMethod, @Nullable String body)
//    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
//    URL url = config.resolveNamespacedURLV3(namespacedId.getNamespace(), path);
//    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
//    if (body != null) {
//      builder.withBody(body);
//    }
//    HttpResponse response = restClient.execute(builder.build(), config.getAccessToken(),
//                                               HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
//    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
//      throw new BadRequestException(response.getResponseBodyAsString());
//    }
//    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
//      throw new NotFoundException(namespacedId);
//    }
//    return response;
//  }
//
//  // construct a component of the path, specific to each entity type
//
//  private String constructPath(Id.Application appId) {
//    return String.format("apps/%s", appId.getId());
//  }
//
//  private String constructPath(Id.Artifact artifactId) {
//    return String.format("artifacts/%s/versions/%s", artifactId.getName(), artifactId.getVersion().getVersion());
//  }
//
//  private String constructPath(Id.Program programId) {
//    return String.format("apps/%s/%s/%s",
//                         programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
//  }
//
//  private String constructPath(Id.Run runId) {
//    Id.Program programId = runId.getProgram();
//    return String.format("apps/%s/%s/%s/runs/%s",
//                         programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId(),
//                         runId.getId());
//  }
//
//  private String constructPath(Id.DatasetInstance datasetInstance) {
//    return String.format("datasets/%s", datasetInstance.getId());
//  }
//
//  private String constructPath(Id.Stream streamId) {
//    return String.format("streams/%s", streamId.getId());
//  }
//
//  private String constructPath(Id.Stream.View viewId) {
//    return String.format("streams/%s/views/%s", viewId.getStreamId(), viewId.getId());
//  }
//}
