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

package co.cask.cdap.apps.schedule;

import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.datacleansing.DataCleansing;
import co.cask.cdap.examples.datacleansing.DataCleansingService;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ScheduleTest extends AudiTestBase {

  @After
  public void after() throws Exception {
    WorkflowId twoActionsWorkflow = new WorkflowId(getConfiguredNamespace().getNamespace(),
                                           AppWithDataPartitionSchedule.NAME,
                                           AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);

    List<RunRecord> suspendedRuns =
      getProgramClient().getProgramRuns(twoActionsWorkflow, "SUSPENDED", 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    for (RunRecord runRecord : suspendedRuns) {
      resumeWorkflow(twoActionsWorkflow, runRecord.getPid(), 200);
    }
  }

  @Test
  public void testSchedulesWithConstraints() throws Exception {
    // Deploy DataCleansing app and start service
    ApplicationManager dataCleansing = deployApplication(DataCleansing.class);
    ServiceManager serviceManager = dataCleansing.getServiceManager(DataCleansingService.NAME).start();
    // Deploy AppWithDataPartitionSchedule
    ApplicationManager appWithSchedule = deployApplication(AppWithDataPartitionSchedule.class);
    final WorkflowManager workflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    final WorkflowManager delayWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.DELAY_WORKFLOW);
    WorkflowManager timeWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW);
    // Resume the schedule since schedules are initialized as suspended
    workflowManager.getSchedule(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE).resume();
    delayWorkflowManager.getSchedule(AppWithDataPartitionSchedule.DELAY_SCHEDULE).resume();
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.ALWAYS_FAIL_SCHEDULE).resume();
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.TEN_SECS_SCHEDULE).resume();
    long oneMinSchedStartTime = System.currentTimeMillis();
    // Start TWO_ACTIONS_WORKFLOW and suspend it
    WorkflowId twoActionsWorkflow = new WorkflowId(getConfiguredNamespace().getNamespace(),
                                                   AppWithDataPartitionSchedule.NAME,
                                                   AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    String runId = workflowManager.getHistory().get(0).getPid();
    suspendWorkflow(twoActionsWorkflow, runId, 200);
    final int numRunsBeforeTrigger = workflowManager.getHistory().size();
    // Create enough new partitions to trigger schedules
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceUrl = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    long timeBeforeTrigger = System.currentTimeMillis();
    triggerDataSchedule(serviceUrl);

    // DELAY_WORKFLOW should not start within DELAY_MILLIS
    final int numDelayWorkflowRuns = delayWorkflowManager.getHistory().size();
    try {
      long timeAfterFirstTrigger = System.currentTimeMillis() - timeBeforeTrigger;
      // DELAY_WORKFLOW should only be launched after DELAY_MILLIS since the first trigger is fired
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return delayWorkflowManager.getHistory().size() > numDelayWorkflowRuns;
        }
      }, AppWithDataPartitionSchedule.DELAY_MILLIS - timeAfterFirstTrigger, TimeUnit.MILLISECONDS,
                    500, TimeUnit.MILLISECONDS);
      Assert.fail(
        String.format("Workflow '%s' should not run within %d millis since the schedule is triggered.",
                      AppWithDataPartitionSchedule.DELAY_WORKFLOW, AppWithDataPartitionSchedule.DELAY_MILLIS));
    } catch (TimeoutException e) {
      // expected
    }
    // DELAY_WORKFLOW should run after DELAY_MILLIS and complete
    delayWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 300, TimeUnit.SECONDS);
    // TWO_ACTIONS_WORKFLOW should not have a new run because of one TWO_ACTIONS_WORKFLOW run is suspended
    // and the concurrency constraint requires only one run of TWO_ACTIONS_WORKFLOW
    Assert.assertEquals(numRunsBeforeTrigger, workflowManager.getHistory().size());
    // Resume TWO_ACTIONS_WORKFLOW should allow triggered TWO_ACTIONS_WORKFLOW to run and complete
    resumeWorkflow(twoActionsWorkflow, runId, 200);
    // Wait for resumed TWO_ACTIONS_WORKFLOW and a new run triggered by CONCURRENCY_SCHEDULE to complete
    waitForRunsWithStatus(workflowManager, ProgramRunStatus.COMPLETED, numRunsBeforeTrigger + 1, 300L);

    // Record number of runs after the scheduled TWO_ACTIONS_WORKFLOW completed
    int numRunsAfterComplete = workflowManager.getHistory().size();
    // Suspend the schedule. Intentionally using a different method from resume to test both manager and client
    ScheduleId concurrencySchedule = TEST_NAMESPACE.app(AppWithDataPartitionSchedule.NAME)
      .schedule(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE);
    new ScheduleClient(getClientConfig(), getRestClient()).suspend(concurrencySchedule);
    // Wait for the TIME_TRIGGER_ONLY_WORKFLOW launched by ALWAYS_FAIL_SCHEDULE, which was triggered
    // by new partitions created previously, so that after new partitions are created again, ALWAYS_FAIL_SCHEDULE
    // can be triggered and TIME_TRIGGER_ONLY_WORKFLOW will have one more failed run
    timeWorkflowManager.waitForRun(ProgramRunStatus.FAILED, 20, TimeUnit.SECONDS);
    // New partitions should not trigger the schedule to launch workflow
    triggerDataSchedule(serviceUrl);
    // Sleep for 10 sec to make sure notifications are processed and TWO_ACTIONS_WORKFLOW is not launched after
    // schedule is suspended
    TimeUnit.SECONDS.sleep(10);
    Assert.assertEquals(numRunsAfterComplete, workflowManager.getHistory().size());
    // Sleep if one minute has not passed since TEN_SECS_SCHEDULE was resumed
    long waitMillis = TimeUnit.MINUTES.toMillis(1) - (System.currentTimeMillis() - oneMinSchedStartTime);
    if (waitMillis > 0) {
      Thread.sleep(waitMillis);
      // Wait for TIME_TRIGGER_ONLY_WORKFLOW to complete
      timeWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 20, TimeUnit.SECONDS);
    } else {
      // Wait for to complete at most 20 seconds after TEN_SECS_SCHEDULE is triggered so that TIME_TRIGGER_ONLY_WORKFLOW
      // can complete
      long waitMillisAfterTimeTrigger = TimeUnit.SECONDS.toMillis(20);
      if (waitMillisAfterTimeTrigger + waitMillis > 0) {
        Thread.sleep(waitMillisAfterTimeTrigger);
      }
    }

    // Wait for 2 failed runs since ALWAYS_FAIL_SCHEDULE should be triggered twice by two calls to triggerDataSchedule
    waitForRunsWithStatus(timeWorkflowManager, ProgramRunStatus.FAILED, 2, 30L);
    // Wait for 1 completed run since TEN_SECS_SCHEDULE is triggered every 10 seconds but can only run
    // TIME_TRIGGER_ONLY_WORKFLOW after MIN_SINCE_LAST_RUN minutes since last
    // completed run of TIME_TRIGGER_ONLY_WORKFLOW
    waitForRunsWithStatus(timeWorkflowManager, ProgramRunStatus.COMPLETED, 1, 30L);
    // Totally only 3 runs with no other status
    Assert.assertEquals(3, timeWorkflowManager.getHistory().size());
  }

  private void waitForRunsWithStatus(final WorkflowManager workflowManager, final ProgramRunStatus status,
                                     int numRuns, long waitSecs)
    throws InterruptedException, ExecutionException, TimeoutException {
    Tasks.waitFor(numRuns, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return workflowManager.getHistory(status).size();
      }
    }, waitSecs, TimeUnit.SECONDS);
  }

  /**
   * Create TRIGGER_ON_NUM_PARTITIONS new partitions with the service in DataCleansing app
   */
  private void triggerDataSchedule(URL serviceUrl)
    throws UnauthorizedException, IOException, UnauthenticatedException {
    for (int i = 0; i < AppWithDataPartitionSchedule.TRIGGER_ON_NUM_PARTITIONS; i++) {
      createPartition(serviceUrl);
    }
  }

  private void createPartition(URL serviceUrl)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    HttpRequest request = HttpRequest.post(url).withBody("new partition").build();
    HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void suspendWorkflow(ProgramId program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("namespaces/%s/apps/%s/workflows/%s/runs/%s/suspend",
                                program.getNamespace(), program.getApplication(), program.getProgram(), runId);
    waitForStatusCode(path, expectedStatusCode);
  }

  /**
   * Tries to resume a Workflow and expect the call completed with the status.
   */
  private void resumeWorkflow(ProgramId program, String runId, int expectedStatusCode) throws Exception {
    String path = String.format("namespaces/%s/apps/%s/workflows/%s/runs/%s/resume",
                                program.getNamespace(), program.getApplication(), program.getProgram(), runId);
    waitForStatusCode(path, expectedStatusCode);
  }

  private void waitForStatusCode(final String path, final int expectedStatusCode)
    throws Exception {
    URL url = getClientConfig().resolveURLV3(path);
    final HttpRequest request = HttpRequest.post(url).build();
    Tasks.waitFor(expectedStatusCode, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
        return response.getResponseCode();
      }
    }, 60, TimeUnit.SECONDS);
  }
}
