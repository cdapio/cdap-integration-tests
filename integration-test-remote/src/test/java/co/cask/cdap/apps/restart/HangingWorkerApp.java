/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.apps.restart;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 *
 */
public class HangingWorkerApp extends AbstractApplication {
  public static final String WORKER_DATASET_NAME = "HangingWorkerDataset";
  public static final String WORKER_NAME = "HangingWorker";
  public static final String WORKER_DATASET_TEST_KEY = "updateTime";
  public static final int WORKER_SLEEP_SECS = 2;

  @Override
  public void configure() {
    setName("HangingWorkerApp");
    setDescription("Application to test restart of non-responding programs");
    createDataset(WORKER_DATASET_NAME, KeyValueTable.class);
    addWorker(new HangingWorker());
  }
}
