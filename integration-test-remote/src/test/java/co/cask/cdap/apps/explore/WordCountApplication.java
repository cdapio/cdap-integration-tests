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

package co.cask.cdap.apps.explore;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.apps.explore.dataset.CounterTable;
import co.cask.cdap.apps.explore.dataset.CounterTableModule;
import co.cask.cdap.apps.explore.dataset.ExtendedCounterTable;
import co.cask.cdap.apps.explore.dataset.ExtendedCounterTableModule;
import co.cask.cdap.apps.explore.dataset.KeyStructValueTable;
import co.cask.cdap.apps.explore.dataset.KeyStructValueTableDefinition;
import co.cask.cdap.apps.explore.dataset.KeyStructValueTableModule;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Simple app with custom dataset.
 */
public class WordCountApplication extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountApplication.class);

  @Override
  public void configure() {
    setDescription("Simple app with table dataset");
    addStream(new Stream("words"));
    addStream(new Stream("words2"));
    addDatasetModule("my_count1", CounterTableModule.class);
    addDatasetModule("my_count_extended", ExtendedCounterTableModule.class);
    createDataset("wordCounts", "my_count_rowscannable_counterTable");
    createDataset("totalCount", "my_count_rowscannable_counterTable");

    createDataset("wordCounts2", "my_count_rowscannable_counterTable");
    createDataset("extendedWordCounts", "my_count_rowscannable_extendedCounterTable");
    addFlow(new ExtendedWordCountFlow());

    addFlow(new WordCountFlow());
    addService(new WordCountService());

    addStream(new Stream("lists"));
    addDatasetModule("kv_table", KeyStructValueTableModule.class);
    createDataset("kvTable", "structKeyValueTable");
    addFlow(new KeyValueFlow());
  }

  /**
   *
   */
  private class KeyValueFlow extends AbstractFlow {

    @Override
    protected void configure() {
      setName("KeyValueFlow");
      setDescription("KeyValueFlow");
      addFlowlet("wordSplitter", new WordSplitterFlowlet());
      connectStream("lists", "wordSplitter");
    }
  }

  /**
   *
   */
  private class WordSplitterFlowlet extends AbstractFlowlet {
    @UseDataSet("kvTable")
    private KeyStructValueTable kvTable;

    @ProcessInput()
    public void process(StreamEvent event) throws CharacterCodingException {
      // str is: key nb1 nb2 nb3 ....
      String str = Charsets.UTF_8.newDecoder().decode(event.getBody()).toString();
      LOG.info("Got string {}", str);
      String[] arr = str.split(" ");
      String key = arr[0];
      LOG.info("Got key {}", key);
      ImmutableList.Builder<Integer> builder = ImmutableList.builder();
      for (int i = 1; i < arr.length; i++) {
        LOG.info("Got number {}", arr[i]);
        builder.add(Integer.parseInt(arr[i]));
      }
      try {
        kvTable.put(key, new KeyStructValueTableDefinition.KeyValue.Value(key, builder.build()));
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  /**
   *
   */
  public static class ExtendedWordCountFlow extends AbstractFlow {

    @Override
    public void configure() {
      setName("ExtendedWordCountFlow");
      setDescription("ExtendedWordCountFlow");
      addFlowlet("wordCounter", new ExtendedWordCountFlowlet());
      connectStream("words2", "wordCounter");

    }
  }

  /**
   *
   */
  public static class ExtendedWordCountFlowlet extends AbstractFlowlet {
    @UseDataSet("extendedWordCounts")
    private ExtendedCounterTable extendedWordCounts;

    @ProcessInput()
    public void process(StreamEvent event) throws CharacterCodingException {
      String str = Charsets.UTF_8.newDecoder().decode(event.getBody()).toString();
      LOG.info("Got string {}", str);
      for (String word : str.split("[ .-]")) {
        LOG.info("Got word {}", word);
        extendedWordCounts.inc(word, 1);
      }
    }
  }

  /**
   *
   */
  public static class WordCountFlow extends AbstractFlow {

    @Override
    public void configure() {
      setName("WordCountFlow");
      setDescription("WordCountFlow");
      addFlowlet("wordCounter", new WordCountFlowlet());
      connectStream("words", "wordCounter");

    }
  }

  /**
   *
   */
  public static class WordCountFlowlet extends AbstractFlowlet {
    @UseDataSet("wordCounts")
    private CounterTable wordCounts;

    @ProcessInput()
    public void process(StreamEvent event) throws CharacterCodingException {
      String str = Charsets.UTF_8.newDecoder().decode(event.getBody()).toString();
      LOG.info("Got string {}", str);
      for (String word : str.split("[ .-]")) {
        LOG.info("Got word {}", word);
        wordCounts.inc(word, 1);
      }
    }
  }

  /**
   *
   */
  public static class WordCountService extends AbstractService {

    @Override
    protected void configure() {
      this.setName("WordCountService");
      this.setDescription("A service to retrieve a customer\'s purchase history");
      this.addHandler(new WordCountHandler());
    }

    /**
     *
     */
    public static final class WordCountHandler extends AbstractHttpServiceHandler {

      @UseDataSet("wordCounts")
      private KeyValueTable wordCounts;

      @UseDataSet("totalCount")
      private KeyValueTable totalCount;

      @GET
      @Path("get/{word}")
      public void get(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("word") String word) {

        byte[] countBytes = wordCounts.read(word);
        long count;
        if (countBytes == null) {
          count = 0;
        } else {
          count = Bytes.toLong(countBytes);
        }

        responder.sendJson(count);
      }

      @GET
      @Path("getTotal")
      public void getTotal(HttpServiceRequest request, HttpServiceResponder responder) {
        byte[] countBytes = totalCount.read("total_words_count");
        long count;
        if (countBytes == null) {
          count = 0;
        } else {
          count = Bytes.toLong(countBytes);
        }

        responder.sendJson(count);
      }
    }
  }
}
