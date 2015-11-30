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

package co.cask.cdap.remote.dataset;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Abstract base class for Applications with Service to interact with Datasets.
 */
public abstract class AbstractDatasetApp extends AbstractApplication<AbstractDatasetApp.DatasetConfig> {

  /**
   * Application's Config which determines the name of the dataset to interact with.
   */
  public static class DatasetConfig extends Config {
    private final String datasetName;

    public DatasetConfig(String datasetName) {
      this.datasetName = datasetName;
    }

    public String getDatasetName() {
      return datasetName;
    }
  }

  /**
   * @return The Class of the Dataset being served by the App.
   */
  protected abstract Class<? extends Dataset> getDatasetClass();

  /**
   * @param datasetName the name of the Dataset to serve.
   * @return An HttpServiceHandler which is responsible for serving the Dataset.
   */
  protected abstract HttpServiceHandler getDatasetHttpHandler(String datasetName);

  @Override
  public void configure() {
    String datasetName = getConfig().getDatasetName();

    // set the name of the application to be the same as the dataset, to avoid conflicting with other datasets' apps
    setName(datasetName);
    addService(new DatasetService(datasetName));
    createDataset(datasetName, getDatasetClass());
  }

  /**
   * Dataset service.
   */
  public class DatasetService extends AbstractService {

    private final String datasetName;

    public DatasetService(String datasetName) {
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      addHandler(getDatasetHttpHandler(datasetName));
      addHandler(new PingHandler());
    }
  }

  /**
   * Ping Handler.
   */
  public static class PingHandler extends AbstractHttpServiceHandler {

    @Path("ping")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }
}
