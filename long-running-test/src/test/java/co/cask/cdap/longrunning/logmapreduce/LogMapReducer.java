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

package co.cask.cdap.longrunning.logmapreduce;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * MapReduce job that reads events from a stream over a given time interval and writes the events out to a FileSet
 * in avro format.
 */
public class LogMapReducer extends AbstractMapReduce {

  private static final Logger LOG = LoggerFactory.getLogger(LogMapReducer.class);
  protected static final String NAME = "LogMapReducer";
  private static String runId;

  private final Map<String, String> dsArguments = Maps.newHashMap();

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Job to read a chunk of stream events and write them to a FileSet");
    setMapperResources(new Resources(512));
    setReducerResources(new Resources(512));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
//    MapReduceContext context = getContext();
    Job job = context.getHadoopJob();
    job.setMapperClass(SCMaper.class);
    job.setReducerClass(SCR.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // read 5 minutes of events from the stream, ending at the logical start time of this run
    long logicalTime = context.getLogicalStartTime();
//    context.addInput(Input.ofStream("events", logicalTime - TimeUnit.MINUTES.toMillis(1), logicalTime));
    StreamBatchReadable.useStreamInput(context, LogMapReduceApp.EVENTS_STREAM,
                                       logicalTime - TimeUnit.MINUTES.toMillis(1), logicalTime);

    runId = context.getRunId().getId();
    context.addOutput(Output.ofDataset("converted", dsArguments));
  }

  /**
   * Mapper that reads events from a stream and writes them out as Avro.
   */
  public static class SCMaper extends
    Mapper<LongWritable, StreamEvent, Text, Text> {

    @Override
    public void map(LongWritable timestamp, StreamEvent streamEvent, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(Bytes.toString(streamEvent.getBody())),
                    new Text(String.valueOf(streamEvent.getTimestamp())));
      LOG.info("mapper {}   {}", runId, Bytes.toString(streamEvent.getBody()));
    }
  }

  /**
   * Reducer class to aggregate all purchases per user
   */
  public static class SCR extends
    Reducer<Text, Text, NullWritable, NullWritable> {
    @Override
    public void reduce(Text timestamp, Iterable<Text> streamEvents, Context context)
      throws IOException, InterruptedException {
      LOG.info("reducer {} {}", runId);
    }
  }
}
