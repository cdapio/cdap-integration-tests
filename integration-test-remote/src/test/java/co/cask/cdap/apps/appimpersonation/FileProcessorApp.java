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

package co.cask.cdap.apps.appimpersonation;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.partitioned.ConsumerConfiguration;
import co.cask.cdap.api.dataset.lib.partitioned.KVTableStatePersistor;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionBatchInput;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This {@code Application} reads files written by {@code FileGeneratorApp} and rewrites them elsewhere.
 */
public class FileProcessorApp extends AbstractApplication {

  private static final String WORKFLOW_NAME = "FileProcessWorkflow";
  private static final String MAPREDUCE_NAME = "FileProcessMapReduce";
  // in milliseconds
  private static final long READ_LAST_N_MINS = 10 * 60 * 1000;

  public static final String GOLD = "X-gold";

  @Override
  public void configure() {
    createDataset("consumingState", KeyValueTable.class);
    createDataset(GOLD, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      // enable explore
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema("record STRING")
      .build());
    addWorkflow(new FileProcessWorkflow());
    addMapReduce(new FileProcessMapReduce());
    scheduleWorkflow(Schedules.builder("Running every 10 min")
                       .setMaxConcurrentRuns(1)
                       .createTimeSchedule("*/10 * * * *"), WORKFLOW_NAME);
  }

  public class FileProcessWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
      addMapReduce(MAPREDUCE_NAME);
    }
  }

  public static class FileProcessMapReduce extends AbstractMapReduce {

    private PartitionBatchInput.BatchPartitionCommitter partitionCommitter;

    @Override
    public void configure() {
      setName(MAPREDUCE_NAME);
    }

    @Override
    public void initialize() throws Exception {
      final MapReduceContext context = getContext();
      PartitionKey outputPartitionKey = PartitionKey.builder().addLongField("time",
                                                                            context.getLogicalStartTime()).build();

      partitionCommitter =
        PartitionBatchInput.setInput(context, FileGeneratorApp.RAW,
                                     new KVTableStatePersistor("consumingState", "state.key"),
                                     ConsumerConfiguration.builder().setPartitionPredicate(
                                       new Predicate<PartitionDetail>() {
                                         @Override
                                         public boolean apply(@Nullable PartitionDetail partitionDetail) {
                                           if (partitionDetail == null) {
                                             return false;
                                           }
                                           long creationTime = partitionDetail.getMetadata().getCreationTime();
                                           long startTime = context.getLogicalStartTime();
                                           return creationTime + READ_LAST_N_MINS > startTime &&
                                             creationTime < startTime;
                                         }}).build());
      Map<String, String> outputArgs = new HashMap<>();
      PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputPartitionKey);
      context.addOutput(Output.ofDataset(FileProcessorApp.GOLD, outputArgs));
      Job job = context.getHadoopJob();
      job.setMapperClass(FileProcessMapper.class);
      job.setNumReduceTasks(0);
    }

    @Override
    public void destroy() {
      boolean isSuccessful = getContext().getState().getStatus() == ProgramStatus.COMPLETED;
      partitionCommitter.onFinish(isSuccessful);
    }
  }

  public static class FileProcessMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(NullWritable.get(), value);
    }
  }
}
