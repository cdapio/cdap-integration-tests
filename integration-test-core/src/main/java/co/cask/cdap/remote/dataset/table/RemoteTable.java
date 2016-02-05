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

package co.cask.cdap.remote.dataset.table;

import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.remote.dataset.TreeMapInstanceCreator;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Remote implementation of {@link co.cask.cdap.api.dataset.table.Table}.
 */
public class RemoteTable extends AbstractRemoteTable {

  private static final Gson GSON = new GsonBuilder()
    .enableComplexMapKeySerialization()
    .registerTypeAdapter(Map.class, new TreeMapInstanceCreator())
    .create();

  private final URL serviceURL;
  private final RESTClient restClient;
  private final ClientConfig clientConfig;

  public RemoteTable(URL serviceURL, RESTClient restClient, ClientConfig clientConfig) {
    this.serviceURL = serviceURL;
    this.restClient = restClient;
    this.clientConfig = clientConfig;
  }

  @Nonnull
  @Override
  public Row get(byte[] row) {
    String json = GSON.toJson(new Get(row));
    return doPost("get", json, Result.class);
  }

  @Nonnull
  @Override
  public Row get(byte[] row, byte[][] columns) {
    String json = GSON.toJson(new Get(row, columns));
    return doPost("get", json, Result.class);
  }

  @Nonnull
  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    String str = GSON.toJson(new GetRequest(row, startColumn, stopColumn, limit));
    return doPost("getWithRange", str, Result.class);
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    Put put = new Put(row);
    for (int i = 0; i < columns.length; i++) {
      put.add(columns[i], values[i]);
    }

    String json = GSON.toJson(put);
    doPost("put", json);
  }

  @Override
  public void delete(byte[] row) {
    String json = GSON.toJson(new Delete(row));
    doPost("delete", json);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    String json = GSON.toJson(new Delete(row, columns));
    doPost("delete", json);
  }

  @Override
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    Increment increment = new Increment(row);
    for (int i = 0; i < columns.length; i++) {
      increment.add(columns[i], amounts[i]);
    }
    String json = GSON.toJson(increment);
    return doPost("incrementAndGet", json, Result.class);
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    // ignore performance
    incrementAndGet(row, columns, amounts);
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] keyColumn, byte[] oldValue, byte[] newValue) {
    String json = GSON.toJson(new CompareAndSwapRequest(key, keyColumn, oldValue, newValue));
    return doPost("compareAndSwap", json, Boolean.class);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  private <T> T doPost(String method, String json, Class<T> clz) {
    HttpResponse response = doPost(method, json);
    return GSON.fromJson(response.getResponseBodyAsString(), clz);
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
