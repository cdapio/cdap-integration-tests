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

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A runner to run Admin calls in a loop with each call running in a separate thread.
 */
public class AdminRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AdminRunner.class);
  private final ExecutorService executorService;
  private final List<Runnable> runnables;

  public AdminRunner(Admin admin, String lookupTable) {
    this.runnables = new ArrayList<>();
    runnables.add(new ExistsRunnable(admin, lookupTable));
    runnables.add(new GetTypeRunnable(admin, lookupTable));
    runnables.add(new GetPropertiesRunnable(admin, lookupTable));
   this.executorService = Executors.newFixedThreadPool(runnables.size());
  }

  public void start() {
    for (Runnable runnable : runnables) {
      executorService.execute(runnable);
    }
  }

  public void stop() {
    executorService.shutdown();
  }

  private abstract static class AdminRunnable implements Runnable {
    protected final Admin admin;
    protected final String dataset;
    private final String op;

    private AdminRunnable(Admin admin, String dataset, String op) {
      this.admin = admin;
      this.dataset = dataset;
      this.op = op;
    }

    @Override
    public void run() {
      while (true) {
        try {
          TimeUnit.SECONDS.sleep(1);
          LOG.info("Start {} dataset {}", op, dataset);
          runCmd();
          LOG.info("Done {} dataset {}", op, dataset);
        } catch (DatasetManagementException e) {
          LOG.error("Error while {} dataset {}", op, dataset, e);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOG.info("Exited while loop inside admin runnable");
    }

    abstract void runCmd() throws DatasetManagementException;
  }

  private static class ExistsRunnable extends AdminRunnable {

    public ExistsRunnable(Admin admin, String dataset) {
      super(admin, dataset, "checking existence of");
    }

    @Override
    void runCmd() throws DatasetManagementException {
      admin.datasetExists(dataset);
    }
  }

  private static class GetTypeRunnable extends AdminRunnable {

    private GetTypeRunnable(Admin admin, String dataset) {
      super(admin, dataset, "getting type of");
    }

    @Override
    void runCmd() throws DatasetManagementException {
      admin.getDatasetType(dataset);
    }
  }

  private static class GetPropertiesRunnable extends AdminRunnable {

    private GetPropertiesRunnable(Admin admin, String dataset) {
      super(admin, dataset, "getting properties of");
    }

    @Override
    void runCmd() throws DatasetManagementException {
      admin.getDatasetProperties(dataset);
    }
  }
}
