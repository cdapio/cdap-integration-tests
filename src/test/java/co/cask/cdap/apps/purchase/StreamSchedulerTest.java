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
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.StreamProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StreamSchedulerTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    /**
     * Set notification threshold for stream PurchaseStream to 1MB
     * Publish more than 1MB to the stream
     * Check that the workflow is executed
     * Suspend the DataSchedule and publish another 1MB of data
     * Ensure that the workflow is triggered after the suspended schedule is resumed
     */
    deployApplication(PurchaseApp.class);
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
    for (int i = 0; i < 12; i++) {
      streamClient.sendEvent(purchaseStream, bigData);
    }
    ProgramClient programClient = getProgramClient();
    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                "RUNNING", 1, TimeUnit.MINUTES);
    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                "RUNNING", 1, TimeUnit.MINUTES);

    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                "STOPPED", 5, TimeUnit.MINUTES);
    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                "STOPPED", 1, TimeUnit.MINUTES);

    // both the mapreduce and workflow should have 1 run
    List<RunRecord> workflowRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(workflowRuns, ProgramRunStatus.COMPLETED);

    List<RunRecord> mapReduceRuns =
      programClient.getAllProgramRuns(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                      0, Long.MAX_VALUE, Integer.MAX_VALUE);
    assertSingleRun(mapReduceRuns, ProgramRunStatus.COMPLETED);


    // schedule actions
    ScheduleClient scheduleClient = new ScheduleClient(getClientConfig(), getRestClient());
    scheduleClient.suspend(PurchaseApp.APP_NAME, "DataSchedule");
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED.name(),
                        scheduleClient.getStatus(PurchaseApp.APP_NAME, "DataSchedule"));

    for (int i = 0; i < 12; i++) {
      streamClient.sendEvent(purchaseStream, bigData);
    }

    scheduleClient.resume(PurchaseApp.APP_NAME, "DataSchedule");
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED.name(),
                        scheduleClient.getStatus(PurchaseApp.APP_NAME, "DataSchedule"));

    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                "RUNNING", 1, TimeUnit.MINUTES);
    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                "RUNNING", 1, TimeUnit.MINUTES);

    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, "PurchaseHistoryBuilder",
                                "STOPPED", 5, TimeUnit.MINUTES);
    programClient.waitForStatus(PurchaseApp.APP_NAME, ProgramType.WORKFLOW, "PurchaseHistoryWorkflow",
                                "STOPPED", 1, TimeUnit.MINUTES);
  }

  private void assertSingleRun(List<RunRecord> runRecords, ProgramRunStatus expectedStatus) {
    Assert.assertEquals(1, runRecords.size());
    Assert.assertEquals(expectedStatus, runRecords.get(0).getStatus());
  }
}
