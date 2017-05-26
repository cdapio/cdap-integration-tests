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

package co.cask.cdap.sportresults;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * A MapReduce program that reads game results and counts statistics per team.
 */
public class ScoreCounter extends AbstractMapReduce {

  private static final Logger LOG = LoggerFactory.getLogger(ScoreCounter.class);

  @Override
  public void configure() {
    setDescription("reads game results and counts statistics per team");
    setMapperResources(new Resources(512));
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();
    Job job = context.getHadoopJob();
    job.setMapperClass(ResultsMapper.class);
    job.setReducerClass(TeamCounter.class);
    job.setNumReduceTasks(1);

    String league = context.getRuntimeArguments().get("league");
    Preconditions.checkNotNull(league);

    String namespace = context.getRuntimeArguments().get("namespace");

    // Configure the input to read all seasons for the league
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setInputPartitionFilter(
      inputArgs, PartitionFilter.builder().addValueCondition("league", league).build());
    context.addInput(Input.ofDataset("results", inputArgs));

    // Each run writes its output to a partition for the league
    Map<String, String> outputArgs = Maps.newHashMap();
    PartitionKey outputKey = PartitionKey.builder().addStringField("league", league).build();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    context.addOutput(Output.ofDataset("totals", outputArgs));

    // used only for logging:
    PartitionedFileSet input;
    PartitionedFileSet outputFileSet;
    if (namespace != null) {
      input = context.getDataset(namespace, "results", inputArgs);
      outputFileSet = context.getDataset(namespace, "totals", outputArgs);
    } else {
      input = context.getDataset("results", inputArgs);
      outputFileSet = context.getDataset("totals", outputArgs);
    }

    String outputPath = FileSetArguments.getOutputPath(outputFileSet.getEmbeddedFileSet().getRuntimeArguments());
    LOG.info("input: {}, output: {}", input.getEmbeddedFileSet().getInputLocations(), outputPath);
  }

  /**
   * The Mapper emits a record with the team name, points scored, and points conceded, for both teams.
   */
  public static class ResultsMapper extends Mapper<LongWritable, Text, Text, GameStat> {
    @Override
    protected void map(LongWritable position, Text value, Context context)
      throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      if (fields.length < 5) {
        return;
      }
      String winner = fields[1];
      String loser = fields[2];
      try {
        int winnerPoints = Integer.parseInt(fields[3]);
        int loserPoints = Integer.parseInt(fields[4]);
        context.write(new Text(winner), new GameStat(winnerPoints, loserPoints));
        context.write(new Text(loser), new GameStat(loserPoints, winnerPoints));
      } catch (NumberFormatException e) {
        LOG.debug("Exception parsing input position {}: {}", position, value.toString());
      }
    }
  }

  /**
   *  The reducer counts all the different statistics.
   */
  public static class TeamCounter extends Reducer<Text, GameStat, Text, String> {
    @Override
    protected void reduce(Text key, Iterable<GameStat> values, Context context)
      throws IOException, InterruptedException {
      int losses = 0, wins = 0, ties = 0, scored = 0, conceded = 0;
      for (GameStat stat : values) {
        if (stat.getScored() > stat.getConceded()) {
          wins++;
        } else if (stat.getScored() < stat.getConceded()) {
          losses++;
        } else {
          ties++;
        }
        scored += stat.getScored();
        conceded += stat.getConceded();
      }
      context.write(key, String.format("%d,%d,%d,%d,%d", wins, ties, losses, scored, conceded));
    }
  }

  /**
   * Private writable helper class used between mappers and reducers.
   */
  private static class GameStat implements Writable {
    private int scored;
    private int conceded;

    @SuppressWarnings("unused")
    GameStat() { }

    GameStat(int scored, int conceded) {
      this.scored = scored;
      this.conceded = conceded;
    }

    public int getScored() {
      return scored;
    }

    public int getConceded() {
      return conceded;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(scored);
      out.writeInt(conceded);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      scored = in.readInt();
      conceded = in.readInt();
    }
  }
}
