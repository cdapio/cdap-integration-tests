/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.examples.appwithsleepingflowlet;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.util.concurrent.TimeUnit;

/**
 * This is a simple application with a flow that contains a flowlet that sleeps as it processes events.
 * The purpose of this sleep is to make it so that the flowlet has pending events on the queue it processes from.
 */
public class AppWithSleepingFlowlet extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithSleepingFlowlet");
    addStream(new Stream("ingestStream"));
    addFlow(new FlowWithSleepingFlowlet());
  }

  /**
   * Sample Flow.
   */
  public static final class FlowWithSleepingFlowlet extends AbstractFlow {
    
    @Override
    protected void configure() {
      setName("FlowWithSleepingFlowlet");
      setDescription("A flow that collects names");
      addFlowlet("streamFlowlet", new StreamFlowlet());
      addFlowlet("sleepingFlowlet", new SleepingFlowlet());
      connectStream("ingestStream", "streamFlowlet");
      connect("streamFlowlet", "sleepingFlowlet");
    }
  }

  /**
   * Flowlet that pulls events from a stream and emits to the next flowlet.
   */
  public static final class StreamFlowlet extends AbstractFlowlet {
    private OutputEmitter<StreamEvent> out;

    @ProcessInput
    public void process(StreamEvent event) {
      out.emit(event);
    }
  }

  /**
   * Flowlet that sleeps each time it processes an element.
   */
  public static final class SleepingFlowlet extends AbstractFlowlet {

    @ProcessInput
    public void process(StreamEvent event) throws InterruptedException {
      TimeUnit.SECONDS.sleep(1);
    }
  }

}
