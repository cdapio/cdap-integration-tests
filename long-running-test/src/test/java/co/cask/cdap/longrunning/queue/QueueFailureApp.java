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

package co.cask.cdap.longrunning.queue;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An app that is designed to cause transaction failures in the queue table.
 */
public class QueueFailureApp extends AbstractApplication<QueueFailureApp.FailConfig> {
  public static final String NAME = "QueueFailureApp";

  @Override
  public void configure() {
    setName(NAME);
    FailConfig conf = getConfig();
    addDatasetModule("failing_tx", FailingTxTableModule.class);
    createDataset(conf.datasetName, FailingTxTable.class, DatasetProperties.EMPTY);
    addFlow(new FailureFlow(conf));
  }

  /**
   * A flow that consumes events from a stream
   */
  public static class FailureFlow extends AbstractFlow {
    public static final String NAME = "FailureFlow";
    private final FailConfig conf;

    public FailureFlow(FailConfig conf) {
      this.conf = conf;
    }

    @Override
    protected void configure() {
      setName(NAME);
      addStream(conf.streamName);
      addFlowlet("reader", new ReaderFlowlet(conf.triggerValue, conf.datasetName));
      addFlowlet("failer", new FailureFlowlet());
      connectStream(conf.streamName, "reader");
      connect("reader", "failer");
    }
  }

  /**
   * Reads from a stream and turns the event into a string. On the trigger event, it sleeps for a while to make sure
   * the transaction will timeout, then sets a flag in the failingTxTable so that it calls System.halt() on its
   * rollback() call. This will cause the queue to persist, the call to the tx service to return an error that the
   * tx has timed out, rollback to occur, then the failingTxTable will kill the process before the queue can
   * rollback.
   */
  public static class ReaderFlowlet extends AbstractFlowlet {
    private OutputEmitter<String> out;
    private String triggerVal;
    private String dsName;
    private FailingTxTable failingTxTable;

    public ReaderFlowlet(String triggerVal, String dsName) {
      this.triggerVal = triggerVal;
      this.dsName = dsName;
    }

    @Override
    protected void configure() {
      Map<String, String> properties = new HashMap<>();
      properties.put("trigger", triggerVal);
      properties.put("dsName", dsName);
      setProperties(properties);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      triggerVal = context.getSpecification().getProperty("trigger");
      failingTxTable = context.getDataset(context.getSpecification().getProperty("dsName"));
    }

    @ProcessInput
    public void process(StreamEvent e, InputContext ctx) throws InterruptedException {
      String val = Bytes.toString(e.getBody());
      if (triggerVal.equals(val)) {
        failingTxTable.setShouldFail(true);
        TimeUnit.SECONDS.sleep(60);
      }
      out.emit(val);
    }
  }

  /**
   * This flowlet only exists so that the previous one has a queue to write to.
   */
  public static class FailureFlowlet extends AbstractFlowlet {

    @ProcessInput
    public void process(String e, InputContext ctx) throws InterruptedException {
      // no-op.
    }
  }

  /**
   * Config for the app. Allows setting the trigger value, dataset name, and stream name.
   */
  public static class FailConfig extends Config {
    public static final String STREAM = "striim";
    public static final String TRIGGER = "failnow";
    public static final String DATASET = "failure";
    private String triggerValue;
    private String datasetName;
    private String streamName;

    public FailConfig() {
      triggerValue = TRIGGER;
      datasetName = DATASET;
      streamName = STREAM;
    }

    public FailConfig(String triggerValue, String datasetName, String streamName) {
      this.triggerValue = triggerValue;
      this.datasetName = datasetName;
      this.streamName = streamName;
    }
  }
}
