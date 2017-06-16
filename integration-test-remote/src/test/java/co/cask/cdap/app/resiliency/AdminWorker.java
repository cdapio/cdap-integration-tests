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

import co.cask.cdap.api.worker.AbstractWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A worker that repeatedly runs Admin calls.
 */
public class AdminWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(AdminWorker.class);
  private AdminRunner adminRunner;
  private boolean isRunning = true;

  @Override
  public void run() {
    LOG.info("Starting admin runner");
    adminRunner = new AdminRunner(getContext().getAdmin(), "testDataset");
    adminRunner.start();
    while (isRunning) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  @Override
  public void stop() {
    adminRunner.stop();
    isRunning = false;
  }
}
