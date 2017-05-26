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

package co.cask.cdap.sportresults;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Map;

/**
 * An example that illustrates using partitioned file sets through an example of sport results analytics.
 */
public class SportResults extends AbstractApplication<SportResults.SportsConfig> {

  public static class SportsConfig extends Config {
    public boolean create = true;
    public String resultPerms = null;
    public String resultGroup = null;
    public String resultGrants = null;
    public String resultDatabase = null;
    public String totalPerms = null;
    public String totalGroup = null;
    public String totalGrants = null;
    public String totalDatabase = null;
  }

  @Override
  public void configure() {
    addService(new UploadService());
    addMapReduce(new ScoreCounter());

    if (!getConfig().create) {
      return;
    }

    // Create the "results" partitioned file set, configure it to work with MapReduce and with Explore
    PartitionedFileSetProperties.Builder builder = PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addStringField("league").addIntField("season").build());
    if (getConfig().resultGrants != null) {
      Map<String, String> grants = new Gson().fromJson(getConfig().resultGrants,
                                                       new TypeToken<Map<String, String>>() {
                                                       }.getType());
      builder.setTablePermissions(grants);
    }
    if (getConfig().resultGroup != null) {
      builder.setFileGroup(getConfig().resultGroup);
    }
    if (getConfig().resultPerms != null) {
      builder.setFilePermissions(getConfig().resultPerms);
    }
    if (getConfig().resultDatabase != null) {
      builder.setExploreDatabaseName(getConfig().resultDatabase);
    }
    builder
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("csv")
      .setExploreSchema("`date` STRING, winner STRING, loser STRING, winnerpoints INT, loserpoints INT")
      .setExploreTableName("results")
      .setDescription("FileSet dataset of game results for a sport league and season");
    createDataset("results", PartitionedFileSet.class, builder.build());

    // Create the aggregates partitioned file set, configure it to work with MapReduce and with Explore
    builder = PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addStringField("league").build());
    if (getConfig().totalGrants != null) {
      Map<String, String> grants = new Gson().fromJson(getConfig().totalGrants,
                                                       new TypeToken<Map<String, String>>() {
                                                       }.getType());
      builder.setTablePermissions(grants);
    }
    if (getConfig().totalGroup != null) {
      builder.setFileGroup(getConfig().totalGroup);
    }
    if (getConfig().totalPerms != null) {
      builder.setFilePermissions(getConfig().totalPerms);
    }
    if (getConfig().totalDatabase != null) {
      builder.setExploreDatabaseName(getConfig().totalDatabase);
    }
    builder
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreTableName("totals")
      .setExploreFormat("csv")
      .setExploreSchema("team STRING, wins INT, ties INT, losses INT, scored INT, conceded INT")
      .setDescription("FileSet dataset of aggregated results for each sport league");
    createDataset("totals", PartitionedFileSet.class, builder.build());
  }
}
