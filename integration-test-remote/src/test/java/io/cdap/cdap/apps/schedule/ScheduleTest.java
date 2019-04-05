/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.apps.schedule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduleTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTest.class);
  private static final String SERVICE_NAME = "DataCleansingService";

  @Before
  public void before() throws Exception {
    // Deploy DataCleansing app and start service
    // (CDAP-14914) ScheduleTest currently fails and isn't run as part of the regularly-scheduled integration tests
    // ApplicationManager dataCleansing = deployApplication(DataCleansing.class);
    // dataCleansing.getServiceManager(SERVICE_NAME).start();
    // Deploy AppWithDataPartitionSchedule
    deployApplication(AppWithDataPartitionSchedule.class);
  }

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
    NamespaceId namespaceId = getConfiguredNamespace();
    ApplicationId appId = namespaceId.app(AppWithDataPartitionSchedule.NAME);
    // Get DataCleansing app manager
    ApplicationManager dataCleansing = getApplicationManager(namespaceId.app("DataCleansing"));
    // Get AppWithDataPartitionSchedule manager
    ApplicationManager appWithSchedule = getApplicationManager(appId);
    final WorkflowManager workflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    final WorkflowManager delayWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.DELAY_WORKFLOW);
    WorkflowManager timeWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW);
    // Resume the schedule since schedules are initialized as suspended
    workflowManager.getSchedule(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE).resume();
    delayWorkflowManager.getSchedule(AppWithDataPartitionSchedule.DELAY_SCHEDULE).resume();
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.CAN_FAIL_SCHEDULE).resume();
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.TIME_SCHEDULE).resume();
    long oneMinSchedStartTime = System.currentTimeMillis();
    // Start TWO_ACTIONS_WORKFLOW and suspend it
    WorkflowId twoActionsWorkflow = new WorkflowId(getConfiguredNamespace().getNamespace(),
                                                   AppWithDataPartitionSchedule.NAME,
                                                   AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    String runId = startAndSuspendWorkflow(workflowManager, twoActionsWorkflow);
    // Create enough new partitions to trigger schedules and record
    // the number of runs and time before sending the trigger
    final int numRunsBeforeTrigger = workflowManager.getHistory().size();
    long timeBeforeTrigger = System.currentTimeMillis();
    URL serviceUrl = dataCleansing.getServiceManager(SERVICE_NAME)
      .getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    triggerDataSchedule(serviceUrl);

    // DELAY_WORKFLOW should not start within DELAY_MILLIS
    final int numDelayWorkflowRuns = delayWorkflowManager.getHistory().size();
    try {
      long timeAfterFirstTrigger = System.currentTimeMillis() - timeBeforeTrigger;
      // DELAY_WORKFLOW should only be launched after DELAY_MILLIS since the first trigger is fired
      Tasks.waitFor(true, () -> delayWorkflowManager.getHistory().size() > numDelayWorkflowRuns,
                    AppWithDataPartitionSchedule.DELAY_MILLIS - timeAfterFirstTrigger, TimeUnit.MILLISECONDS,
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
    // Resume suspended TWO_ACTIONS_WORKFLOW and wait for it to complete
    resumeWorkflow(twoActionsWorkflow, runId, 200);
    // Wait for resumed TWO_ACTIONS_WORKFLOW plus a new run launched by CONCURRENCY_SCHEDULE to complete
    waitForRunsWithStatus(workflowManager, ProgramRunStatus.COMPLETED, numRunsBeforeTrigger + 1, 300L);

    // Record number of runs after the scheduled TWO_ACTIONS_WORKFLOW completed
    int numRunsAfterComplete = workflowManager.getHistory().size();
    // Suspend the schedule. Intentionally using a different method from resume to test both manager and client
    ScheduleId concurrencySchedule = TEST_NAMESPACE.app(AppWithDataPartitionSchedule.NAME)
      .schedule(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE);
    new ScheduleClient(getClientConfig(), getRestClient()).suspend(concurrencySchedule);
    // Wait for the TIME_TRIGGER_ONLY_WORKFLOW launched by CAN_FAIL_SCHEDULE, which was triggered
    // by new partitions created previously, so that after new partitions are created again, CAN_FAIL_SCHEDULE
    // can be triggered and launch TIME_TRIGGER_ONLY_WORKFLOW to have one more failed run
    timeWorkflowManager.waitForRun(ProgramRunStatus.FAILED, 20, TimeUnit.SECONDS);
    // New partitions should not trigger the suspended schedule to launch workflow
    triggerDataSchedule(serviceUrl);
    // Sleep for 10 sec to make sure notifications are processed and TWO_ACTIONS_WORKFLOW is not launched after
    // schedule is suspended
    TimeUnit.SECONDS.sleep(10);
    Assert.assertEquals(numRunsAfterComplete, workflowManager.getHistory().size());
    // Sleep if one minute has not passed since TIME_SCHEDULE was resumed
    long waitMillis = TimeUnit.MINUTES.toMillis(1) - (System.currentTimeMillis() - oneMinSchedStartTime);
    if (waitMillis > 0) {
      Thread.sleep(waitMillis);
      // Wait for TIME_TRIGGER_ONLY_WORKFLOW to complete
      timeWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 20, TimeUnit.SECONDS);
    } else {
      // Wait for to complete at most 20 seconds after TIME_SCHEDULE is triggered so that TIME_TRIGGER_ONLY_WORKFLOW
      // can complete
      long waitMillisAfterTimeTrigger = TimeUnit.SECONDS.toMillis(20);
      if (waitMillisAfterTimeTrigger + waitMillis > 0) {
        Thread.sleep(waitMillisAfterTimeTrigger);
      }
    }

    // Wait for 2 failed runs since CAN_FAIL_SCHEDULE should be triggered twice by two calls to triggerDataSchedule
    timeWorkflowManager.waitForRuns(ProgramRunStatus.FAILED, 2, 30L, TimeUnit.SECONDS);
    // Wait for 1 completed run since TIME_SCHEDULE is triggered every 10 seconds but can only run
    // TIME_TRIGGER_ONLY_WORKFLOW after MIN_SINCE_LAST_RUN minutes since last
    // completed run of TIME_TRIGGER_ONLY_WORKFLOW
    timeWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 30L, TimeUnit.SECONDS);
    // Totally only 3 runs with no other status
    Assert.assertEquals(3, timeWorkflowManager.getHistory().size());
  }

  @Test
  public void testScheduleRestApi() throws Exception {
    NamespaceId namespaceId = getConfiguredNamespace();
    ApplicationId appId = namespaceId.app(AppWithDataPartitionSchedule.NAME);
    // Get app managers, service URL and schedule client
    ApplicationManager dataCleansing = getApplicationManager(namespaceId.app("DataCleansing"));
    URL serviceUrl = dataCleansing.getServiceManager("DataCleansingService")
      .getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    ApplicationManager appWithSchedule = getApplicationManager(appId);
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), getRestClient());

    // Add a schedule triggered by one partition for TIME_TRIGGER_ONLY_WORKFLOW by REST api
    ScheduleId onePartitionSchedule = appId.schedule("OnePartitionSchedule");
    ScheduleDetail onePartitionDetail =
      new ScheduleDetail(onePartitionSchedule.getSchedule(), "one partition",
                         new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW,
                                                 AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW),
                         ImmutableMap.of(),
                         new ProtoTrigger.PartitionTrigger(namespaceId.dataset("rawRecords"), 1),
                         ImmutableList.<Constraint>of(), TimeUnit.HOURS.toMillis(1));
    scheduleClient.add(onePartitionSchedule, onePartitionDetail);
    // OnePartitionSchedule should initially be in SUSPENDED state
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED.name(), scheduleClient.getStatus(onePartitionSchedule));

    // Delete CAN_FAIL_SCHEDULE and TIME_SCHEDULE
    scheduleClient.delete(appId.schedule(AppWithDataPartitionSchedule.CAN_FAIL_SCHEDULE));
    scheduleClient.delete(appId.schedule(AppWithDataPartitionSchedule.TIME_SCHEDULE));
    // Assert that OnePartitionSchedule is the only schedule left for TIME_TRIGGER_ONLY_WORKFLOW
    List<ScheduleDetail> schedules =
      scheduleClient.listSchedules(appId.workflow(AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW));
    Assert.assertEquals(1, schedules.size());
    Assert.assertEquals(onePartitionDetail, schedules.get(0));

    // Resume CONCURRENCY_SCHEDULE
    ScheduleId concurrencySchedule = appId.schedule(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE);
    scheduleClient.resume(concurrencySchedule);

    // Create one new partition
    createPartition(serviceUrl);

    // Start TWO_ACTIONS_WORKFLOW and suspend it, so that after updating CONCURRENCY_SCHEDULE to contain
    // no concurrency constraint, another run of TWO_ACTIONS_WORKFLOW will be launched by CONCURRENCY_SCHEDULE
    // even though there is a suspended run
    final WorkflowManager workflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    WorkflowId twoActionsWorkflow = appId.workflow(AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW);
    startAndSuspendWorkflow(workflowManager, twoActionsWorkflow);

    // Sleep for 5 seconds to wait for the new partition notification is received to trigger CONCURRENCY_SCHEDULE
    TimeUnit.SECONDS.sleep(5);
    // Update CONCURRENCY_SCHEDULE's trigger to be satisfied by 2 partitions and no concurrency constraints
    ScheduleDetail noConcurrencyUpdate =
      new ScheduleDetail(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE, null, null, null,
                         new ProtoTrigger.PartitionTrigger(namespaceId.dataset("rawRecords"), 2),
                         ImmutableList.<Constraint>of(), null);
    scheduleClient.update(concurrencySchedule, noConcurrencyUpdate);
    // CONCURRENCY_SCHEDULE should stay scheduled after update
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED.name(), scheduleClient.getStatus(concurrencySchedule));

    // Creating 1 more partition should not satisfy CONCURRENCY_SCHEDULE's trigger since the first partition created
    // before update is not counted after update
    createPartition(serviceUrl);
    try {
      workflowManager.waitForRun(ProgramRunStatus.RUNNING, 30L, TimeUnit.SECONDS);
      Assert.fail(AppWithDataPartitionSchedule.CONCURRENCY_SCHEDULE + "'s trigger should not be satisfied " +
                    "with one new partition to launch workflow '" +
                    AppWithDataPartitionSchedule.TWO_ACTIONS_WORKFLOW + "'");
    } catch (TimeoutException e) {
      // Expected
    }

    // Resume OnePartitionSchedule
    scheduleClient.resume(onePartitionSchedule);

    // 1 more partition plus the previous one should satisfy OnePartitionSchedule's trigger
    // to launch TIME_TRIGGER_ONLY_WORKFLOW.
    createPartition(serviceUrl);
    // Assert that there is only one run with COMPLETED status for TIME_TRIGGER_ONLY_WORKFLOW
    WorkflowManager timeWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW);
    waitForRunsWithStatus(timeWorkflowManager, ProgramRunStatus.COMPLETED, 1, 60L);
    Assert.assertEquals(1, timeWorkflowManager.getHistory().size());
    // Totally 2 new partitions after updating CONCURRENCY_SCHEDULE are enough to satisfy its trigger to
    // launch TWO_ACTIONS_WORKFLOW even though there is still one suspended run of TWO_ACTIONS_WORKFLOW, since
    // concurrency constraint is removed. Wait for 1 completed run of TWO_ACTIONS_WORKFLOW
    waitForRunsWithStatus(workflowManager, ProgramRunStatus.COMPLETED, 1, 60L);
  }

  @Test
  public void testRedeployApp() throws Exception {
    NamespaceId namespaceId = getConfiguredNamespace();
    // Get a time window starting 2 hours from now so that this time window will never be reached in this test
    Calendar initTime = Calendar.getInstance();
    initTime.add(Calendar.HOUR_OF_DAY, 2);
    String startTime = String.format("%02d:%02d", initTime.get(Calendar.HOUR_OF_DAY), initTime.get(Calendar.MINUTE));
    initTime.add(Calendar.MINUTE, 1);
    String endTime = String.format("%02d:%02d", initTime.get(Calendar.HOUR_OF_DAY), initTime.get(Calendar.MINUTE));
    // Redeploy AppWithDataPartitionSchedule to add update TIME_SCHEDULE with time window constraint
    // and enable CAN_FAIL_SCHEDULE to launch TIME_TRIGGER_ONLY_WORKFLOW successfully
    AppWithDataPartitionSchedule.AppConfig config =
      new AppWithDataPartitionSchedule.AppConfig(startTime, endTime, initTime.getTimeZone().getID());
    ArtifactId artifactId = namespaceId.artifact(AppWithDataPartitionSchedule.NAME, "1.0.0");
    addAppArtifact(artifactId, AppWithDataPartitionSchedule.class);
    AppRequest<? extends Config> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()), config, null, null, true);
    ApplicationId appId = namespaceId.app(AppWithDataPartitionSchedule.NAME);
    ApplicationManager appWithSchedule = deployApplication(appId, request);

    WorkflowManager timeWorkflowManager =
      appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW);
    // Resume CAN_FAIL_SCHEDULE and TIME_SCHEDULE after redeploy
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.CAN_FAIL_SCHEDULE).resume();
    timeWorkflowManager.getSchedule(AppWithDataPartitionSchedule.TIME_SCHEDULE).resume();
    // Create enough new partitions satisfy CAN_FAIL_SCHEDULE's trigger
    final URL serviceUrl = getApplicationManager(namespaceId.app("DataCleansing"))
      .getServiceManager(SERVICE_NAME)
      .getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    triggerDataSchedule(serviceUrl);
    // Wait for a complete run for TIME_TRIGGER_ONLY_WORKFLOW launched by CAN_FAIL_SCHEDULE
    timeWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 60L, TimeUnit.SECONDS);

    // Update CAN_FAIL_SCHEDULE to also have a time window constraint which will abort the job if not met
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), getRestClient());
    initTime = Calendar.getInstance();
    initTime.add(Calendar.MINUTE, 2);
    final Date rangeStartTime = initTime.getTime();
    startTime = String.format("%02d:%02d", initTime.get(Calendar.HOUR_OF_DAY), initTime.get(Calendar.MINUTE));
    initTime.add(Calendar.MINUTE, 1);
    Calendar rangeEnd = Calendar.getInstance();
    rangeEnd.setTime(initTime.getTime());
    endTime = String.format("%02d:%02d", initTime.get(Calendar.HOUR_OF_DAY), initTime.get(Calendar.MINUTE));
    ProtoConstraint.TimeRangeConstraint timeRange =
      new ProtoConstraint.TimeRangeConstraint(startTime, endTime, initTime.getTimeZone());
    timeRange.setWaitUntilMet(false);
    // With WORKFLOW_FAIL_PROPERTY, TIME_TRIGGER_ONLY_WORKFLOW all runs launched by CAN_FAIL_SCHEDULE will fail
    ScheduleId canFaileSchedule = appId.schedule(AppWithDataPartitionSchedule.CAN_FAIL_SCHEDULE);
    ScheduleDetail canFailUpdate =
      new ScheduleDetail(AppWithDataPartitionSchedule.CAN_FAIL_SCHEDULE, null, null,
                         AppWithDataPartitionSchedule.WORKFLOW_FAIL_PROPERTY, null,
                         ImmutableList.<Constraint>of(timeRange),
                         null);
    scheduleClient.update(canFaileSchedule, canFailUpdate);
    // CAN_FAIL_SCHEDULE should stay scheduled after update
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED.name(), scheduleClient.getStatus(canFaileSchedule));

    final Date realRangeEndTime = rangeEnd.getTime();
    long waitTime = rangeEnd.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();
    // Make end of range 10 seconds earlier to ensure that CAN_FAIL_SCHEDULE is triggered within the time range
    rangeEnd.add(Calendar.SECOND, -10);
    final Date preRangeEndTime = rangeEnd.getTime();
    // Create enough new partitions to satisfy CAN_FAIL_SCHEDULE's trigger and record the number of times that
    // CAN_FAIL_SCHEDULE's trigger is satisfied within the time range
    final AtomicInteger numFailedRuns = new AtomicInteger();
    final int sleepSeconds = 20;

    final List<Long> triggerTime = new ArrayList<>();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Calendar now = Calendar.getInstance();
        Date nowTime = now.getTime();
        // create new partitions if the time now is before preRangeEndTime to avoid ambiguity of
        // whether the CAN_FAIL_SCHEDULE's trigger is satisfied when the time is close to the realRangeEndTime
        if (nowTime.before(preRangeEndTime)) {
          triggerDataSchedule(serviceUrl);
          if (nowTime.after(rangeStartTime)) {
            // increment the count of failed runs if the time now is within the time range
            numFailedRuns.incrementAndGet();
            triggerTime.add(nowTime.getTime());
          }
        }
        // prevent timeout happen between the sleep
        now.add(Calendar.SECOND, sleepSeconds + 2);
        return now.getTime().after(realRangeEndTime);
      }
    }, waitTime, TimeUnit.MILLISECONDS, sleepSeconds, TimeUnit.SECONDS);
    int num = numFailedRuns.get();
    Assert.assertTrue(String.format("Expected number of failed runs of '%s' should be greater than zero.",
                                    AppWithDataPartitionSchedule.TIME_TRIGGER_ONLY_WORKFLOW), num > 0);
    // Wait for the runs of TIME_TRIGGER_ONLY_WORKFLOW launched by CAN_FAIL_SCHEDULE during the time range,
    // which are all expected to fail
    try {
      waitForRunsWithStatus(timeWorkflowManager, ProgramRunStatus.FAILED, num,
                            PROGRAM_START_STOP_TIMEOUT_SECONDS * num);
    } catch (TimeoutException e) {
      LOG.error("Triggers sent at time: {}, but received failed run records: {}",
                triggerTime, timeWorkflowManager.getHistory(ProgramRunStatus.FAILED));
      throw e;
    }
  }

  private String startAndSuspendWorkflow(WorkflowManager workflowManager, WorkflowId workflowId) throws Exception {
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    String runId = workflowManager.getHistory().get(0).getPid();
    suspendWorkflow(workflowId, runId, 200);
    return runId;
  }

  /**
   * Wait for the exact {@code numRuns} of a workflow with the given {@code status}.
   * Different from {@link WorkflowManager#waitForRuns(ProgramRunStatus, int, long, TimeUnit)} which waits for
   * the number of runs equal or greater than the given number.
   */
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

  private void waitForStatusCode(final String path, final int expectedStatusCode) throws Exception {
    URL url = getClientConfig().resolveURLV3(path);
    HttpRequest request = HttpRequest.post(url).build();
    Tasks.waitFor(expectedStatusCode,
                  () -> getRestClient().execute(request, getClientConfig().getAccessToken()).getResponseCode(),
                  60, TimeUnit.SECONDS);
  }
}
