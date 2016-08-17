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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.RuntimeContext;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * MapReduce job that reads events from a stream over a given time interval and writes logs.
 *
 */
public class LogMapReduce extends AbstractMapReduce {

  private static final Logger LOG = LoggerFactory.getLogger(LogMapReduce.class);
  protected static final String NAME = "LogMapReduce";
  private final Map<String, String> dsArguments = Maps.newHashMap();

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Job to read a chunk of stream events");
    setMapperResources(new Resources(512));
    setReducerResources(new Resources(512));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Job job = context.getHadoopJob();
    job.setMapperClass(LogMapper.class);
    job.setReducerClass(LogReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    long logicalTime = context.getLogicalStartTime();
    long eventReadStartTime = Long.valueOf(context.getRuntimeArguments().get("eventReadStartTime"));

    StreamBatchReadable.useStreamInput(context, LogMapReduceApp.EVENTS_STREAM,
                                       eventReadStartTime - TimeUnit.SECONDS.toMillis(1), logicalTime);

//    String runId = context.getRunId().getId();
//    LOG.info("RUN ID in init  " + runId);
    context.addOutput(Output.ofDataset("converted", dsArguments));
  }

  /**
   * Mapper that reads events from a stream and writes logs.
   */
  public static class LogMapper extends
    Mapper<LongWritable, StreamEvent, Text, Text> implements ProgramLifecycle {
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private String runId;

    @Override
    public void map(LongWritable timestamp, StreamEvent streamEvent, Context context1)
      throws IOException, InterruptedException {
      context1.write(new Text(Bytes.toString(streamEvent.getBody())),
                    new Text(String.valueOf(streamEvent.getTimestamp())));
      LOG.info("CURRENT mapper {}", runId);
    }

    @Override
    public void initialize(RuntimeContext runtimeContext) throws Exception {
        runId = runtimeContext.getRunId().getId();
    }

    @Override
    public void destroy() {

    }
  }

  /**
   * Reducer class.
   */
  public static class LogReducer extends
    Reducer<Text, Text, NullWritable, NullWritable> implements ProgramLifecycle {
    private String runId;

    @Override
    public void reduce(Text timestamp, Iterable<Text> streamEvents, Context context)
      throws IOException, InterruptedException {
      LOG.info("CURRENT reducer {}", runId);
    }

    @Override
    public void initialize(RuntimeContext runtimeContext) throws Exception {
      runId = runtimeContext.getRunId().getId();
    }

    @Override
    public void destroy() {

    }
  }
}
