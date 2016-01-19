/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.remote.dataset.cube;

import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.remote.dataset.TreeMapInstanceCreator;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collection;
import java.util.Map;

/**
 * Remote implementation of Cube.
 */
public class RemoteCube implements Cube {

  private static final Gson GSON = new GsonBuilder()
    .enableComplexMapKeySerialization()
    .registerTypeAdapter(Map.class, new TreeMapInstanceCreator())
    .create();

  private final URL serviceURL;
  private final RESTClient restClient;
  private final ClientConfig clientConfig;

  public RemoteCube(URL serviceURL, RESTClient restClient, ClientConfig clientConfig) {
    this.serviceURL = serviceURL;
    this.restClient = restClient;
    this.clientConfig = clientConfig;
  }

  @Override
  public void add(CubeFact cubeFact) {
    add(ImmutableList.of(cubeFact));
  }

  @Override
  public void add(Collection<? extends CubeFact> collection) {
    String json = GSON.toJson(collection);
    doPost("add", json);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery cubeQuery) {
    String json = GSON.toJson(cubeQuery);
    return doPost("query", json, new TypeToken<Collection<TimeSeries>>() { }.getType());
  }

  @Override
  public void delete(CubeDeleteQuery cubeDeleteQuery) {
    throw new UnsupportedOperationException("Delete is not supported on Remote Cube.");
  }

  @Override
  public Collection<DimensionValue> findDimensionValues(CubeExploreQuery cubeExploreQuery) {
    String json = GSON.toJson(cubeExploreQuery);
    return doPost("searchDimensionValue", json, new TypeToken<Collection<DimensionValue>>() { }.getType());
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery cubeExploreQuery) {
    String json = GSON.toJson(cubeExploreQuery);
    return doPost("searchMeasure", json, new TypeToken<Collection<String>>() { }.getType());
  }

  @Override
  public void write(Object ignored, CubeFact cubeFact) {
    add(cubeFact);
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

  private <T> T doPost(String method, String json, Type typeOfT) {
    HttpResponse response = doPost(method, json);
    return GSON.fromJson(response.getResponseBodyAsString(), typeOfT);
  }

  private HttpResponse doPost(String method, String json) {
    try {
      URL url = new URL(serviceURL, method);
      return restClient.execute(HttpMethod.POST, url, json,
                                ImmutableMap.<String, String>of(), clientConfig.getAccessToken());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
