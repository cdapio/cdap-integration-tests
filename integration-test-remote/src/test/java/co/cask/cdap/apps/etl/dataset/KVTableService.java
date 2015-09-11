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

package co.cask.cdap.apps.etl.dataset;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.io.IOException;
import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

public class KVTableService extends AbstractService {

  public static final String KV_TABLE_PATH = "kvtable";
  public static final String KV_TABLE_NAME = "kvtable1";
  public static final String KEY = "key";

  @Override
  protected void configure() {
    setDescription("A Service to access Key Value Tables");
    addHandler(new KVTableHandler());
  }

  public static class KVTableHandler extends AbstractHttpServiceHandler {

    @UseDataSet(KV_TABLE_NAME)
    private KeyValueTable kvTableName;

    @GET
    @Path(KV_TABLE_PATH + "/{tableName}")
    public void readKVTable(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("tableName") String tableName, @QueryParam(KEY) String key) throws IOException {

      KeyValueTable table;
      try {
        table = getContext().getDataset(tableName);
      } catch (DatasetInstantiationException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            String.format("Invalid key value table name '%s'", tableName));
        return;
      }

      byte[] readValue = table.read(Bytes.toBytes(key));

      if (readValue == null) {
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, "The given key was not found");
        return;
      }
      responder.sendString(Bytes.toString(readValue));
    }
  }
}
