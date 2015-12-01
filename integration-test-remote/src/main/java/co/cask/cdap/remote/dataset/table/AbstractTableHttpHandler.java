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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * HttpHandler to make API calls  the Table.
 */
public abstract class AbstractTableHttpHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  /**
   * @return {@link Table} dataset.
   */
  protected abstract Table getTable();

  @Path("get")
  @POST
  public void get(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    Get get = deser(request, Get.class);
    Row row = getTable().get(get);
    responder.sendJson(200, row, row.getClass(), GSON);
  }

  @Path("getWithRange")
  @POST
  public void getWithRange(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    GetRequest getRequest = deser(request, GetRequest.class);
    Row row = getTable().get(getRequest.getRow(), getRequest.getStartColumn(),
                             getRequest.getStopColumn(), getRequest.getLimit());
    responder.sendJson(200, row, row.getClass(), GSON);
  }

  @Path("put")
  @POST
  public void put(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    Put put = deser(request, Put.class);
    getTable().put(put);
    responder.sendStatus(200);
  }

  @Path("delete")
  @POST
  public void delete(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    Delete delete = deser(request, Delete.class);
    getTable().delete(delete);
    responder.sendStatus(200);
  }

  @Path("incrementAndGet")
  @POST
  public void incrementAndGet(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    Increment increment = deser(request, Increment.class);
    Row row = getTable().incrementAndGet(increment);
    responder.sendJson(200, row, row.getClass(), GSON);
  }

  @Path("compareAndSwap")
  @POST
  public void compareAndSwap(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    CompareAndSwapRequest compareAndSwap = deser(request, CompareAndSwapRequest.class);
    Boolean succeeded = getTable().compareAndSwap(compareAndSwap.getRow(), compareAndSwap.getColumn(),
                                                  compareAndSwap.getOldValue(), compareAndSwap.getNewValue());
    responder.sendJson(200, succeeded, succeeded.getClass(), GSON);
  }

  private <T> T deser(HttpServiceRequest request, Class<T> clz) {
    return GSON.fromJson(Bytes.toString(request.getContent()), clz);
  }
}
