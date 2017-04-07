/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.resiliency;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class ContinuousCounterService extends AbstractService {

  public static final String SERVICE_NAME = "ContinuousCounterService";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service to retrieve the counter");
    addHandler(new ContinuousCounterServiceHandler());
  }

  public static final class ContinuousCounterServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet(ContinuousCounterApp.TABLE_NAME)
    private KeyValueTable store;

    @Path("allCounter")
    @GET
    public void allCounter(HttpServiceRequest request, HttpServiceResponder responder) {
      List<Integer> values = new ArrayList<>();
      CloseableIterator<KeyValue<byte[], byte[]>> iterator = store.scan(null, null);
      while (iterator.hasNext()) {
        KeyValue<byte[], byte[]> kv = iterator.next();
        String value = new String(kv.getValue());
        values.add(new Integer(value));
      }

      responder.sendJson(HttpURLConnection.HTTP_OK, values);
    }
  }
}
