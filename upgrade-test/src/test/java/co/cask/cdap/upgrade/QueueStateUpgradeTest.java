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

package co.cask.cdap.upgrade;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.examples.appwithsleepingflowlet.AppWithSleepingFlowlet;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.StreamManager;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

/**
 * Tests that queue consuming state is consistent and flows consuming from queues works after an upgrade.
 */
public class QueueStateUpgradeTest extends UpgradeTestBase {
  private static final int EVENT_COUNT = 20;

  @Override
  protected void preStage() throws Exception {
    ApplicationManager appManager = deployApplication(AppWithSleepingFlowlet.class);
    FlowManager flowManager = appManager.getFlowManager("FlowWithSleepingFlowlet").start();
    flowManager.waitForStatus(true, 60, 1);

    RuntimeMetrics streamFlowlet = flowManager.getFlowletMetrics("streamFlowlet");
    RuntimeMetrics sleepingFlowlet = flowManager.getFlowletMetrics("sleepingFlowlet");

    StreamManager streamManager = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "ingestStream"));

    // wait for all the flowlet containers to get processing
    streamManager.send("event0");
    streamFlowlet.waitForProcessed(1, 60, TimeUnit.SECONDS);
    sleepingFlowlet.waitForProcessed(1, 60, TimeUnit.SECONDS);

    // start at i = 1, because we already sent initial event above
    for (int i = 1; i < EVENT_COUNT; i++) {
      streamManager.send("event" + i);
    }

    // wait for the first flowlet to process all the events and place them on the inter-flowlet queue
    streamFlowlet.waitForProcessed(EVENT_COUNT, 60, TimeUnit.SECONDS);

    flowManager.stop();
    flowManager.waitForFinish(60, TimeUnit.SECONDS);

    // after stopping the flow, the second flowlet has not processed all the events yet because of the sleep in its
    // process method
    long sleepingFlowletProcessed = sleepingFlowlet.getProcessed();
    String errMessage = String.format(
      "Expected sleepingFlowlet to not have processed all %s events. It has processed %s events",
      EVENT_COUNT, sleepingFlowletProcessed);
    Assert.assertTrue(errMessage, sleepingFlowletProcessed < EVENT_COUNT);
  }

  @Override
  protected void postStage() throws Exception {
    ApplicationManager appManager = deployApplication(AppWithSleepingFlowlet.class);
    FlowManager flowManager = appManager.getFlowManager("FlowWithSleepingFlowlet");

    RuntimeMetrics streamFlowlet = flowManager.getFlowletMetrics("streamFlowlet");
    RuntimeMetrics sleepingFlowlet = flowManager.getFlowletMetrics("sleepingFlowlet");

    // there should be pending events (as asserted at the end of the pre stage)
    Assert.assertEquals(EVENT_COUNT, streamFlowlet.getProcessed());
    Assert.assertNotEquals(EVENT_COUNT, sleepingFlowlet.getProcessed());

    flowManager.start();
    flowManager.waitForStatus(true, 60, 1);

    // after starting the flow, it should process the events that were on the inter-flowlet queue
    sleepingFlowlet.waitForProcessed(EVENT_COUNT, 60, TimeUnit.SECONDS);

    // it should also be able to process new events
    StreamManager streamManager = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "ingestStream"));
    streamManager.send("event0");
    streamFlowlet.waitForProcessed(EVENT_COUNT + 1, 60, TimeUnit.SECONDS);
    sleepingFlowlet.waitForProcessed(EVENT_COUNT + 1, 60, TimeUnit.SECONDS);
    flowManager.stop();
  }
}
