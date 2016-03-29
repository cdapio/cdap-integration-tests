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

package co.cask.cdap.apps.spark.sparkpagerank;

import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.sparkpagerank.SparkPageRankApp;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of {@link SparkPageRankApp}
 */
@Category({
  HDP20Incompatible.class,
  HDP21Incompatible.class,
  CDH51Incompatible.class,
  CDH52Incompatible.class,
  MapR5Incompatible.class
})
public class SparkPageRankAppTest extends AudiTestBase {
  private static final Gson GSON = new GsonBuilder().
    registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();
  private static final String URL_1 = "http://example.com/page1";
  private static final String URL_2 = "http://example.com/page2";
  private static final String URL_3 = "http://example.com/page3";
  private static final String RANK = "14";
  private static final String TOTAL_PAGES = "1";

  private static final Id.Application SPARK_PAGE_RANK_APP = Id.Application.from(TEST_NAMESPACE, "SparkPageRank");
  private static final Id.Stream STREAM = Id.Stream.from(TEST_NAMESPACE, SparkPageRankApp.BACKLINK_URL_STREAM);
  private static final Id.Service PAGE_RANK_SERVICE = Id.Service.from(
    SPARK_PAGE_RANK_APP, SparkPageRankApp.SERVICE_HANDLERS);
  private static final Id.Program RANKS_COUNTER_PROGRAM = Id.Program.from(
    SPARK_PAGE_RANK_APP, ProgramType.MAPREDUCE, SparkPageRankApp.RanksCounter.class.getSimpleName());
  private static final Id.Program PAGE_RANK_PROGRAM = Id.Program.from(SPARK_PAGE_RANK_APP, ProgramType.SPARK,
                                                                      SparkPageRankApp.PageRankSpark.class
                                                                        .getSimpleName());
  private static final Id.DatasetInstance RANKS_DATASET = Id.DatasetInstance.from(TEST_NAMESPACE, "ranks");
  private static final Id.DatasetInstance RANKS_COUNTS_DATASET = Id.DatasetInstance.from(TEST_NAMESPACE, "rankscount");

  @Test
  public void test() throws Exception {
    final ProgramClient programClient = getProgramClient();

    long startTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    ApplicationManager applicationManager = deployApplication(SparkPageRankApp.class);

    // none of the programs should have any run records
    assertRuns(0, programClient, ProgramRunStatus.ALL, PAGE_RANK_SERVICE, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);

    StreamManager backlinkURLStream = getTestManager().getStreamManager(STREAM);
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_2));
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_3));
    backlinkURLStream.send(Joiner.on(" ").join(URL_2, URL_1));
    backlinkURLStream.send(Joiner.on(" ").join(URL_3, URL_1));

    // Start service
    ServiceManager serviceManager = applicationManager.getServiceManager(PAGE_RANK_SERVICE.getId()).start();
    serviceManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    Assert.assertTrue(serviceManager.isRunning());

    // Start Spark Page Rank and await completion
    SparkManager pageRankManager = applicationManager.getSparkManager(PAGE_RANK_PROGRAM.getId());
    pageRankManager.start();

    // wait until the spark program is running or completes. It completes too fast on standalone to rely on
    // programManager#waitForStatus(true, ...)
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        List<RunRecord> pageRankRuns =
          programClient.getAllProgramRuns(PAGE_RANK_PROGRAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
        if (pageRankRuns.size() != 1) {
          return false;
        }
        ProgramRunStatus status = pageRankRuns.get(0).getStatus();
        return (status == ProgramRunStatus.RUNNING || status == ProgramRunStatus.COMPLETED);
      }
    }, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    pageRankManager.waitForStatus(false, 10 * 60, 1);

    List<RunRecord> sparkRanRecords =
      getRunRecords(1, programClient, PAGE_RANK_PROGRAM,
                    ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE);

    // Start mapreduce and await completion
    MapReduceManager ranksCounterManager = applicationManager.getMapReduceManager(RANKS_COUNTER_PROGRAM.getId());
    ranksCounterManager.start();
    ranksCounterManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    // wait 10 minutes for the mapreduce to execute
    ranksCounterManager.waitForStatus(false, 10 * 60, 1);

    List<RunRecord> mrRanRecords =
      getRunRecords(1, programClient, RANKS_COUNTER_PROGRAM,
                    ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE);

    // mapreduce and spark should have 'COMPLETED' state because they complete on their own with a single run
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);

    URL url = new URL(serviceManager.getServiceURL(),
                      SparkPageRankApp.SparkPageRankServiceHandler.TOTAL_PAGES_PATH + "/" + RANK);
    HttpResponse response = retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(TOTAL_PAGES, response.getResponseBodyAsString());

    url = new URL(serviceManager.getServiceURL(), SparkPageRankApp.SparkPageRankServiceHandler.RANKS_PATH);
    response = retryRestCalls(HttpURLConnection.HTTP_OK,
                              HttpRequest.post(url).withBody("{\"url\":\"" + URL_1 + "\"}").build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(RANK, response.getResponseBodyAsString());

    serviceManager.stop();
    serviceManager.waitForStatus(false, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    List<RunRecord> serviceRanRecords =
      getRunRecords(1, programClient, PAGE_RANK_SERVICE,
                    ProgramRunStatus.KILLED.name(), 0, Long.MAX_VALUE);

    long endTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 100;

    url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                   String.format("streams/%s/lineage?start=%s&end=%s",
                                                                 SparkPageRankApp.BACKLINK_URL_STREAM,
                                                                 startTimeSecs, endTimeSecs));
    LineageRecord expected =
      // When CDAP-3657 is fixed, we will no longer need to use LineageSerializer for serializing.
      // Instead we can direclty use Id.toString() to get the program and data keys.
      LineageSerializer.toLineageRecord(
        startTimeSecs,
        endTimeSecs,
        new Lineage(ImmutableSet.of(
          new Relation(STREAM, PAGE_RANK_PROGRAM, AccessType.READ,
                       RunIds.fromString(sparkRanRecords.get(0).getPid())),
          new Relation(RANKS_DATASET, PAGE_RANK_PROGRAM, AccessType.WRITE,
                       RunIds.fromString(sparkRanRecords.get(0).getPid())),
          new Relation(RANKS_DATASET, RANKS_COUNTER_PROGRAM, AccessType.READ,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(RANKS_COUNTS_DATASET, RANKS_COUNTER_PROGRAM, AccessType.WRITE,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(RANKS_DATASET, PAGE_RANK_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRanRecords.get(0).getPid())),
          new Relation(RANKS_COUNTS_DATASET, PAGE_RANK_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRanRecords.get(0).getPid()))
        )), ImmutableSet.<CollapseType>of());
    testLineage(url, expected);

    url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                   String.format("datasets/%s/lineage?start=%s&end=%s",
                                                                 "ranks",
                                                                 startTimeSecs, endTimeSecs));
    testLineage(url, expected);

    url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                   String.format("datasets/%s/lineage?start=%s&end=%s",
                                                                 "rankscount",
                                                                 startTimeSecs, endTimeSecs));
    testLineage(url, expected);


    // services should 'KILLED' state because they were explicitly stopped with a single run
    assertRuns(1, programClient, ProgramRunStatus.KILLED, PAGE_RANK_SERVICE);
  }

  private void testLineage(URL url, LineageRecord expected) throws IOException, UnauthenticatedException {
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    LineageRecord lineageRecord = GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
    Assert.assertEquals(expected, lineageRecord);
  }
}
