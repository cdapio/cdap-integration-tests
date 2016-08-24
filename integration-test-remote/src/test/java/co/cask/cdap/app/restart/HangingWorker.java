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

package co.cask.cdap.app.restart;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Worker that does not stop when stopped.
 */
public class HangingWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(HangingWorker.class);

  @Override
  public void configure() {
    setName(HangingWorkerApp.WORKER_NAME);
    setInstances(2);
  }

  @SuppressWarnings("InfiniteLoopStatement")
  @Override
  public void run() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Stopping worker {}", getContext().getInstanceId());
      }
    });

    LOG.info("Starting worker with instance {}", getContext().getInstanceId());
    final String key = getKey(getContext().getInstanceId());
    while (true) {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(HangingWorkerApp.WORKER_DATASET_NAME);
            table.write(key, Bytes.toBytes(System.currentTimeMillis()));
            TimeUnit.SECONDS.sleep(HangingWorkerApp.WORKER_SLEEP_SECS);
          }
        });
      } catch (Exception e) {
        LOG.error("Got exception", e);
      }
    }
  }

  public static String getKey(int instanceId) {
    return Joiner.on("-").join(HangingWorkerApp.WORKER_DATASET_TEST_KEY, instanceId);
  }
}
