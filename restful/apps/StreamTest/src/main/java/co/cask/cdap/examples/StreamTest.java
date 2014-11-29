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
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

/**
 * This is a simple example that uses one stream, one dataset, one flow and one procedure.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A procedure that reads the name from the KeyValueTable and prints Hello [Name]!</li>
 * </uL>
 */
public class StreamTest extends AbstractApplication {
  @Override
  public void configure() {
      setName("StreamTest");
      setDescription("An app to test some failure scenarios for streams");
      addStream(new Stream("input"));
      createDataset("counts", KeyValueTable.class);
      addFlow(new CountFlow());
      addProcedure(new GetCount());
  }

  /**
   * Test Flow.
   */
  public static class CountFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder
        .with()
        .setName("CountFlow")
        .setDescription("A flow that counts events and words.")
        .withFlowlets()
          .add("reader", new InputReader())
          .add("counter", new WordCounter())
        .connect()
          .fromStream("input").to("reader")
          .from("reader").to("counter")
        .build();
    }
  }

  /**
   * Input Reader Flowlet.
   */
  public static class InputReader extends AbstractFlowlet {
    @Output("wordOut")
    private OutputEmitter<String> tokenOutput;
    Metrics flowletMetrics;

    @ProcessInput
    public void process(StreamEvent event) {
      flowletMetrics.count("events", 1);
      String content = Charsets.UTF_8.decode(event.getBody()).toString();
      for (String token : content.split(" ")) {
        tokenOutput.emit(token);
      }
    }
  }

  static final byte[] ALL_COUNT = { ' ' }; // now token can have blank -> unique

  /**
   * Word Counter Flowlet.
   */
  public static class WordCounter extends AbstractFlowlet {

    @UseDataSet("counts")
    private KeyValueTable counts;

    @ProcessInput
    public void process(String token) {
      if (token != null && !token.isEmpty()) {
        counts.increment(Bytes.toBytes(token), 1L);
      }
      counts.increment(ALL_COUNT, 1L);
    }
  }

  /**
   * Get Counts Procedure.
   */
  public static class GetCount extends AbstractProcedure {

    @UseDataSet("counts")
    private KeyValueTable counts;

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      byte[] key = Bytes.toBytes(request.getArgument("token"));
      byte[] value = counts.read(key);
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), value == null ? ImmutableMap.of() :
        ImmutableMap.of("count", Bytes.toLong(value)));
    }

    @Handle("total")
    public void total(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      byte[] value = counts.read(ALL_COUNT);
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), value == null ? ImmutableMap.of() :
        ImmutableMap.of("count", Bytes.toLong(value)));
    }
  }
}

