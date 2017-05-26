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

package co.cask.cdap.apps.purchase;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of workflows triggered by stream data.
 */
public class StreamSchedulerTest extends AudiTestBase {
  private static final WorkflowId PURCHASE_HISTORY_WORKFLOW =
    TEST_NAMESPACE.app(PurchaseApp.APP_NAME).workflow("PurchaseHistoryWorkflow");
  private static final ProgramId PURCHASE_HISTORY_BUILDER =
    TEST_NAMESPACE.app(PurchaseApp.APP_NAME).mr("PurchaseHistoryBuilder");

  @Test
  public void test() throws Exception {
    /**
     * Set notification threshold for stream PurchaseStream to 1MB
     * Publish more than 1MB to the stream
     * Check that the workflow is executed
     * Suspend the DataSchedule and publish another 1MB of data
     * Ensure that the workflow is triggered after the suspended schedule is resumed
     */
    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    StreamId purchaseStream = TEST_NAMESPACE.stream("purchaseStream");
    StreamProperties purchaseStreamProperties = streamClient.getConfig(purchaseStream);
    StreamProperties streamPropertiesToSet =
      new StreamProperties(purchaseStreamProperties.getTTL(), purchaseStreamProperties.getFormat(), 1);
    streamClient.setStreamProperties(purchaseStream, streamPropertiesToSet);
    purchaseStreamProperties = streamClient.getConfig(purchaseStream);
    // if the test is running in an impersonated namespace then the stream principal will be inherited from
    // the namespace principal and the properties will include that
    String nsPrincipal = getNamespaceClient().get(TEST_NAMESPACE).getConfig().getPrincipal();
    StreamProperties expectedStreamProperties = new StreamProperties(streamPropertiesToSet.getTTL(),
                                                                     streamPropertiesToSet.getFormat(), 1, null,
                                                                     nsPrincipal);
    Assert.assertEquals(expectedStreamProperties, purchaseStreamProperties);
    Assert.assertEquals((Integer) 1, purchaseStreamProperties.getNotificationThresholdMB());

    // schedule actions: Initially schedules are suspended so resume them
    ScheduleId dataSchedule = TEST_NAMESPACE.app(PurchaseApp.APP_NAME).schedule("DataSchedule");
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), getRestClient());
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED.name(), scheduleClient.getStatus(dataSchedule));
    scheduleClient.resume(dataSchedule);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED.name(), scheduleClient.getStatus(dataSchedule));

    String bigData = new String(new char[100000]);
    multipleStreamSend(streamClient, purchaseStream, bigData, 12);

    WorkflowManager purchaseHistoryWorkflow = applicationManager.getWorkflowManager("PurchaseHistoryWorkflow");

    purchaseHistoryWorkflow.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // wait 5 minutes for the map reduce to execute
    purchaseHistoryWorkflow.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // both the mapreduce and workflow should have 1 run
    ProgramClient programClient = getProgramClient();
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);

    // schedule actions suspend, send more data and resume the schedule
    scheduleClient.suspend(dataSchedule);
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED.name(), scheduleClient.getStatus(dataSchedule));

    multipleStreamSend(streamClient, purchaseStream, bigData, 12);

    scheduleClient.resume(dataSchedule);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED.name(), scheduleClient.getStatus(dataSchedule));

    purchaseHistoryWorkflow.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // wait 5 minutes for the mapreduce to execute
    purchaseHistoryWorkflow.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);

    assertRuns(2, programClient, ProgramRunStatus.COMPLETED, PURCHASE_HISTORY_WORKFLOW, PURCHASE_HISTORY_BUILDER);
  }

  private void multipleStreamSend(StreamClient streamClient, StreamId streamId, String event,
                                  int count) throws Exception {
    for (int i = 0; i < count; i++) {
      streamClient.sendEvent(streamId, event);
    }
  }

}
