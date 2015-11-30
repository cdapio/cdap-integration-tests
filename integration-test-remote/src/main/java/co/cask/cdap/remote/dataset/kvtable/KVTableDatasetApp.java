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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;

/**
 * Application which allows reading or writing to a {@link KeyValueTable}.
 */
public class KVTableDatasetApp extends AbstractDatasetApp {

  @Override
  protected Class<? extends Dataset> getDatasetClass() {
    return KeyValueTable.class;
  }

  @Override
  protected HttpServiceHandler getDatasetHttpHandler(String datasetName) {
    return new KVTableHttpHandler(datasetName);
  }

  /**
   * HttpHandler to make API calls on the KeyValueTable.
   */
  public static class KVTableHttpHandler extends AbstractKVTableHttpHandler {

    @Property
    private final String datasetName;

    public KVTableHttpHandler(String datasetName) {
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      useDatasets(datasetName);
    }

    @Override
    protected KeyValueTable getKVTable() {
      return getContext().getDataset(datasetName);
    }
  }
}
