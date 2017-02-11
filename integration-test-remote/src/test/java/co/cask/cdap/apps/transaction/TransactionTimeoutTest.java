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

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TransactionTimeoutTest extends AudiTestBase {

  @Test
  public void testFlowletTimeouts() throws Exception {
    ApplicationManager appManager = deployApplication(TEST_NAMESPACE, TransactionTimeoutApp.class);

    // tx timeout for flow (and flowlet b) is 20 seconds, for flowlet a it is 2 seconds
    FlowManager flowManager =
      appManager.getFlowManager(TransactionTimeoutApp.TimeoutFlow.class.getSimpleName());
    flowManager.start(ImmutableMap.of("flowlet." + TransactionTimeoutApp.FLOWLET_A + ".system.data.tx.timeout", "2",
                                      "system.data.tx.timeout", "20"));

    // tx timeout for workflow (and action d) is 20 seconds, for action c it is 2 seconds
    WorkflowManager workflowManager =
      appManager.getWorkflowManager(TransactionTimeoutApp.TimeoutWorkflow.class.getSimpleName());
    workflowManager.start(ImmutableMap.of("action." + TransactionTimeoutApp.ACTION_C + ".system.data.tx.timeout", "2",
                                          "system.data.tx.timeout", "20"));

    // tx timeout for worker is 2 seconds
    WorkerManager workerManager =
      appManager.getWorkerManager(TransactionTimeoutApp.TimeoutWorker.class.getSimpleName());
    workerManager.start(ImmutableMap.of("system.data.tx.timeout", "2"));

    // make each flowlet sleep 15 seconds. Flowlet a should time out after 2 + 10 seconds, flowlet b has 20 to succeed
    flowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    getTestManager().getStreamManager(TEST_NAMESPACE.stream(TransactionTimeoutApp.SECONDS_STREAM)).send("15");

    RuntimeMetrics aMetrics = flowManager.getFlowletMetrics(TransactionTimeoutApp.FLOWLET_A);
    RuntimeMetrics bMetrics = flowManager.getFlowletMetrics(TransactionTimeoutApp.FLOWLET_B);
    aMetrics.waitForProcessed(1, 90, TimeUnit.SECONDS);
    bMetrics.waitForProcessed(1, 90, TimeUnit.SECONDS);

    flowManager.stop();
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // each flowlet records how often it failed: a timed out once, b succeeded first time
    validateCheckedValues(
      ImmutableMap.<String, String>builder()
        .put(TransactionTimeoutApp.programINIT(TransactionTimeoutApp.FLOWLET_A),    "1")
        .put(TransactionTimeoutApp.programRUNTIME(TransactionTimeoutApp.FLOWLET_A), "1")
        .put(TransactionTimeoutApp.programINIT(TransactionTimeoutApp.FLOWLET_B),    "0")
        .put(TransactionTimeoutApp.programRUNTIME(TransactionTimeoutApp.FLOWLET_B), "0")

        // each action records how often it failed: c timed out once, d succeeded first time
        .put(TransactionTimeoutApp.programINIT(TransactionTimeoutApp.ACTION_C),    "1")
        .put(TransactionTimeoutApp.programRUNTIME(TransactionTimeoutApp.ACTION_C), "1")
        .put(TransactionTimeoutApp.programINIT(TransactionTimeoutApp.ACTION_D),    "0")
        .put(TransactionTimeoutApp.programRUNTIME(TransactionTimeoutApp.ACTION_D), "0")

        // worker records success/failure for various attempts
        .put(TransactionTimeoutApp.WORKER_INIT,    "1") // initialize failed first with timeout of 2secs
        .put(TransactionTimeoutApp.WORKER_RUNTIME, "1") // run failed first with 2 seconds timeout
        .put(TransactionTimeoutApp.WORKER_CUSTOM,  "0") // run then succeeds with custom tx timeout
        .put(TransactionTimeoutApp.WORKER_NESTED,  "1") // nested tx fails
        .build());
  }

  private void validateCheckedValues(Map<String, String> expected) throws Exception {
    Map<String, String> actual = new HashMap<>();
    try (Connection client = getTestManager().getQueryClient(TEST_NAMESPACE)) {
      try (ResultSet results =
        client.prepareStatement(
          "select cast(key as string), cast(value as string) from " + TransactionTimeoutApp.CHECK_DATASET)
          .executeQuery()) {
        while (results.next()) {
          actual.put(results.getString(1), results.getString(2));
        }
      }
    }
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      Assert.assertEquals("Value for '" + entry.getKey() + "' not as expected",
                          entry.getValue(), actual.get(entry.getKey()));
    }
  }
}

