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

package co.cask.cdap.apps.serviceworker;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.worker.AbstractWorker;

import java.util.concurrent.TimeUnit;

/**
 * worker that writes to a dataset.
 */
public class DatasetWorker extends AbstractWorker {
  public static final String WORKER_DATASET_TEST_KEY = "testKey";
  public static final String WORKER_DATASET_TEST_VALUE = "testValue";

  @Override
  public void configure() {
    setName(ServiceApplication.WORKER_NAME);
  }
  @Override
  public void run() {
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        KeyValueTable table = context.getDataset(ServiceApplication.WORKER_DATASET_NAME);
        table.write(WORKER_DATASET_TEST_KEY, WORKER_DATASET_TEST_VALUE);
        TimeUnit.SECONDS.sleep(2);
      }
    });
  }
}

