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
import co.cask.cdap.examples.helloworld.HelloWorld;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.StreamManager;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests that a flow consuming from a stream works after an upgrade (for events ingested before and after the upgrade).
 */
public class StreamStateUpgradeTest extends UpgradeTestBase {
  @Override
  protected void preStage() throws Exception {
    ApplicationManager applicationManager = deployApplication(HelloWorld.class);
    StreamManager whoStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "who"));
    whoStream.send("test1");

    FlowManager flowManager = applicationManager.getFlowManager("WhoFlow").start();
    flowManager.waitForStatus(true, 60, 1);

    // it should process the stream event
    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("saver");
    flowletMetrics.waitForProcessed(1, 60, TimeUnit.SECONDS);

    try {
      flowletMetrics.waitForProcessed(2, 30, TimeUnit.SECONDS);
      Assert.fail("Expected not to receive more than one stream event in the flowlet");
    } catch (TimeoutException expected) {
    }

    flowManager.stop();
    flowManager.waitForFinish(60, TimeUnit.SECONDS);

    // send an additional event, which should be processed after the upgrade
    whoStream.send("test2");
  }

  @Override
  protected void postStage() throws Exception {
    ApplicationManager applicationManager = deployApplication(HelloWorld.class);

    FlowManager flowManager = applicationManager.getFlowManager("WhoFlow");
    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("saver");
    // currently has processed 1
    Assert.assertEquals(1, flowletMetrics.getProcessed());

    flowManager.start();
    flowManager.waitForStatus(true, 60, 1);

    // it should process the second stream event
    flowletMetrics.waitForProcessed(2, 60, TimeUnit.SECONDS);

    try {
      flowletMetrics.waitForProcessed(3, 30, TimeUnit.SECONDS);
      Assert.fail("Expected not to receive more two stream events in the flowlet");
    } catch (TimeoutException expected) {
    }

    StreamManager whoStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "who"));
    whoStream.send("test3");

    // it should process the third (new) stream event
    flowletMetrics.waitForProcessed(3, 60, TimeUnit.SECONDS);

    flowManager.stop();
    flowManager.waitForFinish(60, TimeUnit.SECONDS);
  }
}
