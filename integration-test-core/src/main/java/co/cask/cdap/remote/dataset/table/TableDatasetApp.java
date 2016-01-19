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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;

/**
 * Application which allows reading or writing to a {@link Table}.
 */
public class TableDatasetApp extends AbstractDatasetApp {

  @Override
  protected Class<? extends Dataset> getDatasetClass() {
    return Table.class;
  }

  @Override
  protected HttpServiceHandler getDatasetHttpHandler(String datasetName) {
    return new TableHttpHandler(datasetName);
  }

  /**
   * HttpHandler to make API calls  the Table.
   */
  public static class TableHttpHandler extends AbstractTableHttpHandler {

    @Property
    private final String datasetName;

    public TableHttpHandler(String datasetName) {
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      useDatasets(datasetName);
    }

    @Override
    protected Table getTable() {
      return getContext().getDataset(datasetName);
    }
  }
}
