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

package co.cask.cdap.remote.dataset.cube;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;

/**
 * Application which allows reading or writing to a {@link Cube}.
 */
public class CubeDatasetApp extends AbstractDatasetApp {

  @Override
  protected Class<? extends Dataset> getDatasetClass() {
    return Cube.class;
  }

  @Override
  protected HttpServiceHandler getDatasetHttpHandler(String datasetName) {
    return new CubeHttpHandler(datasetName);
  }

  /**
   * A basic implementation of {@link co.cask.cdap.api.service.http.HttpServiceHandler} that provides endpoints to
   * explore and execute queries in {@link Cube} dataset.
   */
  public static class CubeHttpHandler extends AbstractCubeHttpHandler {

    @Property
    private final String datasetName;

    public CubeHttpHandler(String datasetName) {
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      useDatasets(datasetName);
    }

    @Override
    protected Cube getCube() {
      return getContext().getDataset(datasetName);
    }
  }
}
