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

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class AdminPersistenceService extends AbstractService {
  public static final String NAME = AdminPersistenceService.class.getSimpleName();

  @Override
  protected void configure() {
    setName(NAME);
    addHandler(new AdminHandler());
  }

  public static class AdminHandler extends AbstractHttpServiceHandler {

    @TransactionPolicy(TransactionControl.EXPLICIT)
    @POST
    @Path("ds/{name}")
    public void createDataset(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("name") String dsName) throws DatasetManagementException {
      getContext().getAdmin().createDataset(dsName, "table", DatasetProperties.EMPTY);
      responder.sendStatus(200);
    }

    @TransactionPolicy(TransactionControl.EXPLICIT)
    @GET
    @Path("ds/{name}/properties")
    public void getDatasetProperties(HttpServiceRequest request, HttpServiceResponder responder,
                                     @PathParam("name") String dsName) throws DatasetManagementException {
      responder.sendJson(200, getContext().getAdmin().getDatasetProperties(dsName));
    }

    @TransactionPolicy(TransactionControl.EXPLICIT)
    @GET
    @Path("ds/{name}/type")
    public void getDatasetType(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("name") String dsName) throws DatasetManagementException {
      responder.sendJson(200, getContext().getAdmin().getDatasetType(dsName));
    }
  }
}
