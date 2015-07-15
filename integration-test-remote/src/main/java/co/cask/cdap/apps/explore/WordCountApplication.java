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

package co.cask.cdap.apps.explore;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
//    addMapReduce(new CountTotalsJob());

    addStream(new Stream("lists"));
    addDatasetModule("kv_table", KeyStructValueTableModule.class);
    createDataset("kvTable", "structKeyValueTable");
    addFlow(new KeyValueFlow());
  }

  private class KeyValueFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("KeyValueFlow").setDescription("KeyValueFlow")
        .withFlowlets().add("wordSplitter", new WordSplitterFlowlet())
        .connect().from(new Stream("lists")).to("wordSplitter")
        .build();
    }
  }

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


  public static class ExtendedWordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("ExtendedWordCountFlow").setDescription("ExtendedWordCountFlow")
        .withFlowlets().add("wordCounter", new ExtendedWordCountFlowlet())
        .connect().from(new Stream("words2")).to("wordCounter")
        .build();
    }
  }

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

  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow").setDescription("WordCountFlow")
        .withFlowlets().add("wordCounter", new WordCountFlowlet())
        .connect().from(new Stream("words")).to("wordCounter")
        .build();
    }
  }

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


  public static class WordCountService extends AbstractService {

    @Override
    protected void configure() {
      this.setName("WordCountService");
      this.setDescription("A service to retrieve a customer\'s purchase history");
      this.addHandler(new WordCountHandler());
    }

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


  public static class CountTotalsJob extends AbstractMapReduce {
    @Override
    public void configure() {
      setName("CountTotalsJob");
      setDescription("Counts total words count");
      setInputDataset("wordCounts");
      setOutputDataset("totalCount");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyMapper.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(LongWritable.class);
      job.setReducerClass(MyReducer.class);
    }

    public static class MyMapper extends Mapper<String, Long, BytesWritable, LongWritable> {
      @Override
      protected void map(String key, Long value, Context context) throws IOException, InterruptedException {
        context.write(new BytesWritable(Bytes.toBytes("total")), new LongWritable(value));
      }
    }

    public static class MyReducer extends Reducer<BytesWritable, LongWritable, String, Long> {
      @Override
      protected void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        long total = 0;
        for (LongWritable longWritable : values) {
          total += longWritable.get();
        }

        context.write("total_words_count", total);
      }
    }
  }

}