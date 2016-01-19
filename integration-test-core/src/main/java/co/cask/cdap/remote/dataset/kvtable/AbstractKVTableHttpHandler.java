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

package co.cask.cdap.remote.dataset.kvtable;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * HttpHandler to make API calls on the KeyValueTable.
 */
public abstract class AbstractKVTableHttpHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  /**
   * @return {@link KeyValueTable} dataset.
   */
  protected abstract KeyValueTable getKVTable();

  @Path("read")
  @POST
  public void read(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    byte[] key = deser(request, byte[].class);
    byte[] read = getKVTable().read(key);
    responder.sendJson(new Result(read));
  }

  @Path("readAll")
  @POST
  public void readAll(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    byte[][] keys = deser(request, byte[][].class);
    Map<byte[], byte[]> values = getKVTable().readAll(keys);
    responder.sendJson(200, values, values.getClass(), GSON);
  }

  @Path("incrementAndGet")
  @POST
  public void incrementAndGet(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    IncrementRequest increment = deser(request, IncrementRequest.class);
    long value = getKVTable().incrementAndGet(increment.getKey(), increment.getAmount());
    responder.sendJson(value);
  }

  @Path("write")
  @POST
  public void write(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    WriteRequest write = deser(request, WriteRequest.class);
    getKVTable().write(write.getKey(), write.getValue());
    responder.sendStatus(200);
  }

  @Path("delete")
  @POST
  public void delete(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    byte[] key = deser(request, byte[].class);
    getKVTable().delete(key);
    responder.sendStatus(200);
  }

  @Path("compareAndSwap")
  @POST
  public void compareAndSwap(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    CompareAndSwapRequest compareAndSwapRequest = deser(request, CompareAndSwapRequest.class);
    boolean succeeded = getKVTable().compareAndSwap(compareAndSwapRequest.getRow(),
                                                    compareAndSwapRequest.getOldValue(),
                                                    compareAndSwapRequest.getNewValue());
    responder.sendJson(succeeded);
  }

  private <T> T deser(HttpServiceRequest request, Class<T> clz) {
    return GSON.fromJson(Bytes.toString(request.getContent()), clz);
  }
}
