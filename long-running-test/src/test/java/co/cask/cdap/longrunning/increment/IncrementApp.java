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

package co.cask.cdap.longrunning.increment;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IncrementApp extends AbstractApplication {
  public static final Logger LOG = LoggerFactory.getLogger(IncrementApp.class);
  public static final byte[] SUM_KEY = {'s'};
  public static final byte[] NUM_KEY = {'n'};

  public static final String NAME = "IncrementApp";
  public static final String INT_STREAM = "intStream";
  public static final String READLESS_TABLE = "readlessTable";
  public static final String REGULAR_TABLE = "regularTable";

  @Override
  public void configure() {
    setName(NAME);
    addStream(new Stream(INT_STREAM));
    addFlow(new IncrementFlow());
  }

  public static final class IncrementFlow extends AbstractFlow {
    public static final String NAME = "IncrementFlow";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("A flow reads integers from stream, and increments them");
      addFlowlet("streamFlowlet", new StreamFlowlet());
      addFlowlet("queueFlowlet", new QueueFlowlet());
      connectStream(INT_STREAM, "streamFlowlet");
      connect("streamFlowlet", "queueFlowlet");

      createDataset(READLESS_TABLE, KeyValueTable.class, DatasetProperties.builder()
        .add(Table.PROPERTY_READLESS_INCREMENT, "true").build());
      createDataset(REGULAR_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
    }
  }

  public static final class StreamFlowlet extends AbstractFlowlet {
    private OutputEmitter<Integer> out;

    @UseDataSet(READLESS_TABLE)
    private KeyValueTable readlessTable;

    @ProcessInput
    public void process(StreamEvent event) {
      int value = Integer.parseInt(Bytes.toString(event.getBody()));
      LOG.info("Got int = {}", value);
      readlessTable.increment(SUM_KEY, value);
      readlessTable.increment(NUM_KEY, 1);
      out.emit(value);
    }
  }

  public static final class QueueFlowlet extends AbstractFlowlet {
    @UseDataSet(REGULAR_TABLE)
    private KeyValueTable regularTable;

    @ProcessInput
    public void process(Integer value) throws InterruptedException {
      regularTable.incrementAndGet(SUM_KEY, value);
      regularTable.incrementAndGet(NUM_KEY, 1);
    }
  }
}
