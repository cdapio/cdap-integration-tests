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

package io.cdap.cdap.longrunning.schedule;

import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.LongRunningTestBase;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Schedule long running test which creates new partitions in datasets and assert that
 * scheduled workflows complete successfully.
 */
public class ScheduleLongRunningTest extends LongRunningTestBase<DataPartitionScheduleTestState> {

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), DataPartitionApp.class);
  }

  @Override
  public void start() throws Exception {
    ApplicationManager dataPartition = getApplicationManager(DataPartitionApp.NAME);
    ServiceManager serviceManager = dataPartition.getServiceManager(AddDataPartitionService.NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Resume schedules in the deployed app
    ApplicationId appId = getLongRunningNamespace().app(DataPartitionApp.NAME);
    for (int i = 0; i < DataPartitionApp.NUM_SCHEDULES; i++) {
      ScheduleId scheduleId = appId.schedule(DataPartitionApp.DATASET_PARTITION_SCHEDULE_PREFIX + i);
      new ScheduleClient(getClientConfig(), getRestClient()).resume(scheduleId);
    }
  }

  @Override
  public void stop() throws Exception {
    ServiceManager serviceManager = getApplicationManager(DataPartitionApp.NAME)
      .getServiceManager(AddDataPartitionService.NAME);
    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // Suspend schedules in the deployed app
    ApplicationId appId = getLongRunningNamespace().app(DataPartitionApp.NAME);
    for (int i = 0; i < DataPartitionApp.NUM_SCHEDULES; i++) {
      ScheduleId scheduleId = appId.schedule(DataPartitionApp.DATASET_PARTITION_SCHEDULE_PREFIX + i);
      new ScheduleClient(getClientConfig(), getRestClient()).suspend(scheduleId);
    }
  }

  private ApplicationManager getApplicationManager(String appName) throws Exception {
    return getApplicationManager(getLongRunningNamespace().app(appName));
  }

  @Override
  public DataPartitionScheduleTestState getInitialState() {
    return new DataPartitionScheduleTestState(0, 0);
  }

  @Override
  public void awaitOperations(final DataPartitionScheduleTestState state) throws Exception {
    // Directly return if there's no previous run
    if (state.getExpectedCompletedRunsNum() == 0) {
      return;
    }
    final ProgramClient workflowClient = new ProgramClient(getClientConfig(), getRestClient());
    final ApplicationId appId = getLongRunningNamespace().app(DataPartitionApp.NAME);

    // Wait until no workflow is running
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        for (int i = 0; i < DataPartitionApp.NUM_SCHEDULES; i++) {
          WorkflowId workflowId = appId.workflow(DataPartitionApp.WORKFLOW_PREFIX + i);
          int activeRuns = workflowClient.getProgramRuns(workflowId, ProgramRunStatus.RUNNING.name(),
                                                         state.getTriggerStartTimeSeconds(), Long.MAX_VALUE, 10).size();
          if (activeRuns > 0) {
            return false;
          }
        }
        return true;
      }
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }

  @Override
  public DataPartitionScheduleTestState verifyRuns(DataPartitionScheduleTestState state) throws Exception {
    // Directly return if there's no previous run
    if (state.getExpectedCompletedRunsNum() == 0) {
      return state;
    }
    ApplicationManager appManager = getApplicationManager(DataPartitionApp.NAME);

    // Verify that every workflow is completed for once since triggers were fired in last run
    for (int i = 0; i < DataPartitionApp.NUM_SCHEDULES; i++) {
      WorkflowManager workflowManager = appManager.getWorkflowManager(DataPartitionApp.WORKFLOW_PREFIX + i);
      workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, state.getExpectedCompletedRunsNum(), 5, TimeUnit.MINUTES);
    }
    return state;
  }

  @Override
  public DataPartitionScheduleTestState runOperations(DataPartitionScheduleTestState state) throws Exception {
    ServiceManager serviceManager = getApplicationManager(DataPartitionApp.NAME)
      .getServiceManager(AddDataPartitionService.NAME);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    long triggerStartTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // create new partitions in datasets that can trigger schedules
    // also create new partitions in datasets that won't trigger schedules to test
    // scheduler's ability to handle irrelevant notifications
    for (int datasetSuffix = 0; datasetSuffix < DataPartitionApp.NUM_TRIGGER_DATASET; datasetSuffix++) {
      String datasetName = DataPartitionApp.TRIGGER_DATASET_PREFIX + datasetSuffix;
      for (int i = 0; i < DataPartitionApp.TRIGGER_ON_NUM_PARTITIONS; i++) {
        createPartition(serviceURL, datasetName);
        // create new partitions in NO_TRIGGER_DATASET to send notification that won't trigger schedule
        createPartition(serviceURL, DataPartitionApp.NO_TRIGGER_DATASET);
      }
      for (int j = 0; j < 20; j++) {
        // create new partitions in NO_TRIGGER_DATASET to send notification that won't trigger schedule
        createPartition(serviceURL, DataPartitionApp.NO_TRIGGER_DATASET);
      }
    }
    return new DataPartitionScheduleTestState(triggerStartTime, 1 + state.getExpectedCompletedRunsNum());
  }

  private void createPartition(URL serviceUrl, String datasetName)
    throws IOException {
    URL url = new URL(serviceUrl, "v1/records/" + datasetName);
    HttpRequest request = HttpRequest.post(url).withBody("new partition").build();
    HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }
}
