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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * Example Application that has a Service.
 */
public class ServiceApplication extends AbstractApplication {
  public static final String WORKER_DATASET_NAME = "ServiceWorkerDataset";
  public static final String SERVICE_NAME = "HttpService";
  public static final String WORKER_NAME = "DatasetWorker";

  @Override
  public void configure() {
    setName("ServiceApplication");
    setDescription("Example Application with Services and Workers");
    createDataset(WORKER_DATASET_NAME, KeyValueTable.class);
    addService(new HttpService());
    addWorker(new DatasetWorker());
  }
}
