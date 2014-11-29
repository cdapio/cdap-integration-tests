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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A simple app to count tokens in the input stream.
 */
public class FlowTestApp extends AbstractApplication {

  @Override
  public void configure() {
      setName("FlowTestApp");
      setDescription("FlowTestApp");
      addStream(new Stream("flowin"));
      createDataset("counter", Table.class);
      addFlow(new SimpleFlow());
      addProcedure(new CheckProcedure());
  }

  public static final class SimpleFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SimpleFlow")
        .setDescription("SimpleFlow")
        .withFlowlets()
          .add(new InputFlowlet())
          .add(new EndFlowlet())
        .connect()
          .fromStream("flowin").to(new InputFlowlet())
          .from(new InputFlowlet()).to(new EndFlowlet())
        .build();
    }
  }

  public static final class InputFlowlet extends AbstractFlowlet {
    private OutputEmitter<Integer> output;
    private final Random random = new Random();

    @ProcessInput
    public void process(StreamEvent event) throws InterruptedException {
      TimeUnit.MILLISECONDS.sleep(random.nextInt(50) + 51);
      int i = Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString());
      output.emit(i, "hash", i);
    }
  }

  private static final byte[] COLUMN = "c".getBytes(Charset.forName("UTF-8"));

  public static final class EndFlowlet extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(EndFlowlet.class);

    @UseDataSet("counter")
    private Table counter;

    @ProcessInput
    @HashPartition("hash")
    public void process(int value) {
      counter.increment(Bytes.toBytes(value), COLUMN, 1L);
    }
  }

  public static final class CheckProcedure extends AbstractProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CheckProcedure.class);

    @UseDataSet("counter")
    private Table counter;

    @Handle("check")
    public void check(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      int size = request.getArgument("size", Integer.class);

      for (int i = 1; i <= size; i++) {
        if (Bytes.toLong(counter.get(Bytes.toBytes(i), COLUMN)) != 1L) {
          responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), "Counter for " + i + " != 1.");
          return;
        }
      }

      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    }
  }
}
