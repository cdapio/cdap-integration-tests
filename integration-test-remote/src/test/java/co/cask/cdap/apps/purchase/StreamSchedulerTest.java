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

import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests the functionality of workflows triggered by stream data.
 */
public class StreamSchedulerTest extends AudiTestBase {
  private static final Id.Program PURCHASE_HISTORY_WORKFLOW =
    Id.Program.from(Constants.DEFAULT_NAMESPACE_ID, PurchaseApp.APP_NAME,
                    ProgramType.WORKFLOW, "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER =
    Id.Program.from(Constants.DEFAULT_NAMESPACE_ID, PurchaseApp.APP_NAME,
                    ProgramType.MAPREDUCE, "PurchaseHistoryBuilder");

  @Test
  public void test() throws Exception {
    /**
     * Set notification threshold for stream PurchaseStream to 1MB
     * Publish more than 1MB to the stream
     * Check that the workflow is executed
     * Suspend the DataSchedule and publish another 1MB of data
     * Ensure that the workflow is triggered after the suspended schedule is resumed
     */
    ApplicationManager applicationManager = deployApplication("PurchaseApp.jar");
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    String purchaseStream = "purchaseStream";
    StreamProperties purchaseStreamProperties = streamClient.getConfig(purchaseStream);
    StreamProperties streamPropertiesToSet =
      new StreamProperties(purchaseStreamProperties.getTTL(), purchaseStreamProperties.getFormat(), 1);
    streamClient.setStreamProperties(purchaseStream, streamPropertiesToSet);
    purchaseStreamProperties = streamClient.getConfig(purchaseStream);
    Assert.assertEquals(streamPropertiesToSet, purchaseStreamProperties);
    Assert.assertEquals((Integer) 1, purchaseStreamProperties.getNotificationThresholdMB());

    String bigData = new String(new char[100000]);
    multipleStreamSend(streamClient, purchaseStream, bigData, 12);

    WorkflowManager purchaseHistoryWorkflow = applicationManager.getWorkflowManager("PurchaseHistoryWorkflow");
    MapReduceManager purchaseHistoryBuilder = applicationManager.getMapReduceManager("PurchaseHistoryBuilder");

    purchaseHistoryWorkflow.waitForStatus(true, 60, 1);
    purchaseHistoryBuilder.waitForStatus(true, 60, 1);
    purchaseHistoryBuilder.waitForStatus(false, 5 * 60, 1);
    purchaseHistoryWorkflow.waitForStatus(false, 60, 1);

    // both the mapreduce and workflow should have 1 run
    ProgramClient programClient = getProgramClient();
    assertRuns(1, programClient, PURCHASE_HISTORY_WORKFLOW, ProgramRunStatus.COMPLETED);
    assertRuns(1, programClient, PURCHASE_HISTORY_BUILDER, ProgramRunStatus.COMPLETED);

    // schedule actions
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), getRestClient());
    scheduleClient.suspend(PurchaseApp.APP_NAME, "DataSchedule");
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED.name(),
                        scheduleClient.getStatus(PurchaseApp.APP_NAME, "DataSchedule"));

    multipleStreamSend(streamClient, purchaseStream, bigData, 12);

    scheduleClient.resume(PurchaseApp.APP_NAME, "DataSchedule");
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED.name(),
                        scheduleClient.getStatus(PurchaseApp.APP_NAME, "DataSchedule"));

    purchaseHistoryWorkflow.waitForStatus(true, 60, 1);
    purchaseHistoryBuilder.waitForStatus(true, 60, 1);
    purchaseHistoryBuilder.waitForStatus(false, 5 * 60, 1);
    purchaseHistoryWorkflow.waitForStatus(false, 60, 1);

    assertRuns(2, programClient, PURCHASE_HISTORY_WORKFLOW, ProgramRunStatus.COMPLETED);
    assertRuns(2, programClient, PURCHASE_HISTORY_BUILDER, ProgramRunStatus.COMPLETED);
  }

  private void multipleStreamSend(StreamClient streamClient, String streamName, String event,
                                  int count) throws Exception {
    for (int i = 0; i < count; i++) {
      streamClient.sendEvent(streamName, event);
    }
  }

  // {@param} expectedStatus can be null if count is 0
  private void assertRuns(int count, ProgramClient programClient, Id.Program programId,
                          @Nullable ProgramRunStatus expectedStatus) throws Exception {
    List<RunRecord> runRecords =
      programClient.getAllProgramRuns(programId.getApplicationId(), programId.getType(), programId.getId(),
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(count, runRecords.size());
    for (int i = 0; i < count; i++) {
      Assert.assertEquals(expectedStatus, runRecords.get(i).getStatus());
    }
  }
}
