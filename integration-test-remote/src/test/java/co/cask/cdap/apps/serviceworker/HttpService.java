/*
 * Copyright 2015 Cask, Inc.
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

package co.cask.cdap.apps.serviceworker;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HttpService that exposes GET and POST /ping.
 */
public class HttpService extends AbstractService {


  @Override
  protected void configure() {
    setName(ServiceApplication.SERVICE_NAME);
    addHandler(new ExampleHandler());
  }

  public class ExampleHandler extends AbstractHttpServiceHandler {

    @UseDataSet(ServiceApplication.WORKER_DATASET_NAME)
    KeyValueTable table;

    @Path("/read/{key}")
    @GET
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("key") String key) {
      String value = Bytes.toString(table.read(key));
      if (value == null) {
        responder.sendStatus(204);
      } else {
        responder.sendJson(200, value);
      }
    }

    @Path("ping")
    @GET
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendJson(200, "OK");
    }

  }
}
