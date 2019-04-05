/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.remote.dataset.kvtable;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.RecordScanner;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.remote.dataset.TreeMapInstanceCreator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A key/value map implementation on top of {@link Table} supporting read, write and delete operations.
 */
public class RemoteKeyValueTable extends KeyValueTable {

  private static final Gson GSON = new GsonBuilder()
    .enableComplexMapKeySerialization()
    .registerTypeAdapter(Map.class, new TreeMapInstanceCreator())
    .create();

  private final URL serviceURL;
  private final RESTClient restClient;
  private final ClientConfig clientConfig;

  public RemoteKeyValueTable(URL serviceURL, RESTClient restClient, ClientConfig clientConfig) {
    // fine to pass null, since we never use those fields
    super(null, null);
    this.serviceURL = serviceURL;
    this.restClient = restClient;
    this.clientConfig = clientConfig;
  }

  @Nullable
  @Override
  public byte[] read(String key) {
    return read(Bytes.toBytes(key));
  }

  @Nullable
  @Override
  public byte[] read(byte[] key) {
    String json = GSON.toJson(key);
    return doPost("read", json, Result.class).getResult();
  }

  @Override
  public Map<byte[], byte[]> readAll(byte[][] keys) {
    String json = GSON.toJson(keys);
    HttpResponse response = doPost("readAll", json);
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Map<byte[], byte[]>>() { }.getType());
  }

  @Override
  public long incrementAndGet(byte[] key, long value) {
    String json = GSON.toJson(new IncrementRequest(key, value));
    return doPost("incrementAndGet", json, Long.class);
  }

  @Override
  public void write(byte[] key, byte[] value) {
    String json = GSON.toJson(new WriteRequest(key, value));
    doPost("write", json);
  }

  @Override
  public void write(String key, String value) {
    write(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  @Override
  public void write(String key, byte[] value) {
    write(Bytes.toBytes(key), value);
  }

  @Override
  public void write(KeyValue<byte[], byte[]> keyValue) throws IOException {
    write(keyValue.getKey(), keyValue.getValue());
  }

  @Override
  public void increment(byte[] key, long amount) {
    // ignore performance
    incrementAndGet(key, amount);
  }

  @Override
  public void delete(byte[] key) {
    String json = GSON.toJson(key);
    doPost("delete", json);
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] oldValue, byte[] newValue) {
    String json = GSON.toJson(new CompareAndSwapRequest(key, oldValue, newValue));
    return doPost("compareAndSwap", json, Boolean.class);
  }

  @Override
  public Type getRecordType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Split> getSplits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordScanner<KeyValue<byte[], byte[]>> createSplitRecordScanner(Split split) {
    throw new UnsupportedOperationException();
  }

  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    throw new UnsupportedOperationException();
  }


  @Override
  public CloseableIterator<KeyValue<byte[], byte[]>> scan(byte[] startRow, byte[] stopRow) {
    throw new UnsupportedOperationException();
  }

  private <T> T doPost(String method, String json, Class<T> clz) {
    HttpResponse response = doPost(method, json);
    return GSON.fromJson(response.getResponseBodyAsString(), clz);
  }

  private HttpResponse doPost(String method, String json) {
    try {
      URL url = new URL(serviceURL, method);
      return restClient.execute(HttpMethod.POST, url, json,
                                ImmutableMap.of(), clientConfig.getAccessToken());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
