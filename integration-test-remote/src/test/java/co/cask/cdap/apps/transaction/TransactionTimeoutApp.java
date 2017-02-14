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

package co.cask.cdap.apps.transaction;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.ExploreProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Throwables;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class TransactionTimeoutApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionTimeoutApp.class);

  public static final String CHECK_DATASET = "checks";
  public static final String SECONDS_STREAM = "seconds";

  public static final String FLOWLET_A = "flowletA";
  public static final String FLOWLET_B = "flowletB";
  public static final String ACTION_C = "actionC";
  public static final String ACTION_D = "actionD";
  public static final String WORKER = "worker";

  public static final String INIT = "-init";
  public static final String RUNTIME = "-runtime";

  public static String programINIT(String program) {
    return program + INIT;
  }
  public static String programRUNTIME(String program) {
    return program + RUNTIME;
  }

  public static final String WORKER_INIT = programINIT(WORKER);
  public static final String WORKER_RUNTIME = programRUNTIME(WORKER);
  public static final String WORKER_CUSTOM = programRUNTIME(WORKER) + "-custom";
  public static final String WORKER_NESTED = programRUNTIME(WORKER) + "-nested";


  @Override
  public void configure() {
    createDataset(CHECK_DATASET, KeyValueTable.class,
                  ExploreProperties.builder().setExploreTableName(CHECK_DATASET).build());
    addFlow(new TimeoutFlow());
    addWorkflow(new TimeoutWorkflow());
    addWorker(new TimeoutWorker());
  }

  static void recordCheck(DatasetContext context, String key, String value) {
    LOG.info("Logging check {} = {}", key, value);
    ((KeyValueTable) context.getDataset(CHECK_DATASET)).write(key, value);
  }

  static void executeRecordCheck(Transactional txnl, final String key, final String value,
                                 @Nullable final Integer sleepSeconds) throws TransactionFailureException {
    txnl.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        recordCheck(datasetContext, key, value);
        if (sleepSeconds != null) {
          TimeUnit.SECONDS.sleep(sleepSeconds);
        }
      }
    });
  }

  static void tryRecordCheck(Transactional txnl, String key, Integer sleepSeconds)
    throws TransactionFailureException {
    try {
      executeRecordCheck(txnl, key, "0", sleepSeconds);
    } catch (TransactionFailureException e) {
      executeRecordCheck(txnl, key, "1", null);
    }
  }

  public static class TimeoutFlow extends AbstractFlow {

    @Override
    protected void configure() {
      addStream(SECONDS_STREAM);
      addFlowlet(FLOWLET_A, new TimeoutFlowlet());
      addFlowlet(FLOWLET_B, new TimeoutFlowlet());
      connectStream(SECONDS_STREAM, FLOWLET_A);
      connectStream(SECONDS_STREAM, FLOWLET_B);
    }
  }

  public static class TimeoutFlowlet extends AbstractFlowlet {

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      tryRecordCheck(getContext(), programINIT(getContext().getName()), 15);
    }

    @ProcessInput
    public void process(StreamEvent e, InputContext ctx) throws InterruptedException {
      int seconds = Integer.parseInt(Bytes.toString(e.getBody()));
      // if retry count is 0, then this is the first attempt, and we want to sleep.
      // the next retry will have retry count of 1, and we want the write to go through.
      // this way, the flowlet will record the number of times it failed
      recordCheck(getContext(), programRUNTIME(getContext().getName()), Integer.toString(ctx.getRetryCount()));
      if (ctx.getRetryCount() == 0) {
        TimeUnit.SECONDS.sleep(seconds);
      }
    }
  }

  public static class TimeoutWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addAction(new TimeoutAction(ACTION_C));
      addAction(new TimeoutAction(ACTION_D));
    }
  }

  public static class TimeoutAction extends AbstractCustomAction {

    String name;

    TimeoutAction(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      super.configure();
      setName(name);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize() throws Exception {
      super.initialize();
      tryRecordCheck(getContext(), programINIT(getContext().getSpecification().getName()), 15);
    }

    @Override
    public void run() throws Exception {
      tryRecordCheck(getContext(), programRUNTIME(getContext().getSpecification().getName()), 15);
    }
  }

  public static class TimeoutWorker extends AbstractWorker {

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      tryRecordCheck(context, WORKER_INIT, 15);
    }

    @Override
    public void run() {
      try {
        // with default tx timeout this will fail if timeout is <5 (15 - timeout interval)
        tryRecordCheck(getContext(), WORKER_RUNTIME, 15);

        // now explicitly give a longer timeout, should succeed right away
        getContext().execute(20, new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            recordCheck(datasetContext, WORKER_CUSTOM, "0");
            TimeUnit.SECONDS.sleep(15);
          }
        });

        // attempt a nested transaction
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            try {
              executeRecordCheck(getContext(), WORKER_NESTED, "0", null);
            } catch (TransactionFailureException e) {
              recordCheck(datasetContext, WORKER_NESTED, "1");
            }
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
