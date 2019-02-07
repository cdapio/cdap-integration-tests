/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.apps.workflow;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Workflow app with fork writing to dataset.
 */
public class WorkflowAppWithFork extends AbstractApplication {
  public static final String SCHED_NAME = "testSchedule";
  public static final String DATASET_NAME_ONE = "datasetNameOne";
  public static final String DATASET_NAME_TWO = "datasetNameTwo";

  @Override
  public void configure() {
    setDescription("Workflow App containing fork.");
    createDataset(DATASET_NAME_ONE, Table.class,
                  TableProperties.builder()
                    .setReadlessIncrementSupport(true)
                    .setDescription("Dataset one")
                    .build());
    createDataset(DATASET_NAME_TWO, Table.class,
                  TableProperties.builder()
                    .setReadlessIncrementSupport(true)
                    .setDescription("Dataset two")
                    .build());
    addWorkflow(new WorkflowWithFork());
    schedule(buildSchedule(SCHED_NAME, ProgramType.WORKFLOW, "WorkflowWithFork")
               .setDescription("testDescription")
               .triggerByTime("* * * * *"));
  }

  /**
   *
   */
  public static class WorkflowWithFork extends AbstractWorkflow {

    @Override
    public void configure() {
      setDescription("A workflow that tests forks.");
      addAction(new SimpleActionMain("main"));
      fork()
        .addAction(new SimpleActionIncrement("branch1"))
        .also()
        .addAction(new SimpleActionSecond("branch2"))
        .join();
    }
  }

  public static final class SimpleActionIncrement extends AbstractCustomAction {
    public static final long INCREMENT_AMOUNT = 3L;

    SimpleActionIncrement(String name) {
      super(name);
    }

    @Override
    public void run() {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) {
            Table table = getContext().getDataset(DATASET_NAME_ONE);
            table.increment(new Increment("row").add("col", INCREMENT_AMOUNT));
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static final class SimpleActionMain extends AbstractCustomAction {
    public static final long MAIN_PUT_AMOUNT = 2L;
    SimpleActionMain(String name) {
      super(name);
    }

    @Override
    public void run() {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) {
            Table table = getContext().getDataset(DATASET_NAME_ONE);
            table.put(new Put("row").add("col", MAIN_PUT_AMOUNT));
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static final class SimpleActionSecond extends AbstractCustomAction {
    public static final long BRANCH_PUT_AMOUNT = 20L;
    SimpleActionSecond(String name) {
      super(name);
    }

    @Override
    public void run() {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) {
            Table table = getContext().getDataset(DATASET_NAME_TWO);
            table.put(new Put("row").add("col", BRANCH_PUT_AMOUNT));
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
