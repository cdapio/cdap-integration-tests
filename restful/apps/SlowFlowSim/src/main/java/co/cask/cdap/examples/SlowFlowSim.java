/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.examples;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * A simple example to simulate a flowlet takes long time to process.
 */
public class SlowFlowSim extends AbstractApplication {
  @Override
  public void configure() {
    setName("SlowFlowSim");
    setDescription("Slow Flow Processing Application");
    addStream(new Stream("event"));
    addFlow(new SlowFlow());
  }

  /**
   * A Flow contains one flowlet taking long processing time.
   */
  public static class SlowFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SlowFlow")
        .setDescription("A simulation of long time processing flow")
        .withFlowlets()
        .add("reader", new Reader())
        .connect()
        .fromStream("event").to("reader")
        .build();
    }
  }

  /**
   * A Flowlet simulates taking long processing time.
   */
  public static class Reader extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);
    @ProcessInput
    public void process(StreamEvent event) {
      try {
        TimeUnit.SECONDS.sleep(15);
        String inputString = Charset.forName("UTF-8").decode(event.getBody()).toString();
        LOG.info("inputString:{}", inputString);
      } catch (InterruptedException e) {
        LOG.info("Sleep is interrupted.");
      }
    }
  }
}

