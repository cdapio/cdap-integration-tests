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
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of {@link SparkPageRankApp}
 */
@Category({
  // Currently, coopr doesn't provision MapR cluster with Spark. Enable this test once COOK-108 is fixed
  MapR5Incompatible.class
})
public class SparkPageRankAppTest extends AudiTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();
  private static final String URL_1 = "http://example.com/page1";
  private static final String URL_2 = "http://example.com/page2";
  private static final String URL_3 = "http://example.com/page3";
  private static final String RANK = "14";
  private static final String TOTAL_PAGES = "1";

  private static final ApplicationId SPARK_PAGE_RANK_APP = TEST_NAMESPACE.app("SparkPageRank");
  private static final StreamId STREAM = TEST_NAMESPACE.stream(SparkPageRankApp.BACKLINK_URL_STREAM);
  private static final ProgramId PAGE_RANK_SERVICE = SPARK_PAGE_RANK_APP.service(SparkPageRankApp.SERVICE_HANDLERS);
  private static final ProgramId RANKS_COUNTER_PROGRAM = SPARK_PAGE_RANK_APP.mr(
    SparkPageRankApp.RanksCounter.class.getSimpleName());
  private static final ProgramId PAGE_RANK_PROGRAM = SPARK_PAGE_RANK_APP.spark(
    SparkPageRankApp.PageRankSpark.class.getSimpleName());
  private static final DatasetId RANKS_DATASET = TEST_NAMESPACE.dataset("ranks");
  private static final DatasetId RANKS_COUNTS_DATASET = TEST_NAMESPACE.dataset("rankscount");

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
    ServiceManager serviceManager = applicationManager.getServiceManager(PAGE_RANK_SERVICE.getEntityName()).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    Assert.assertTrue(serviceManager.isRunning());

    // Start Spark Page Rank and await completion
    SparkManager pageRankManager = applicationManager.getSparkManager(PAGE_RANK_PROGRAM.getEntityName());
    Map<String, String> runtimeArgs = ImmutableMap.of("task.client.system.resources.memory", "1024");
    pageRankManager.start(runtimeArgs);

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
    pageRankManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    List<RunRecord> sparkRanRecords =
      getRunRecords(1, programClient, PAGE_RANK_PROGRAM,
                    ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE);

    // Start mapreduce and await completion
    MapReduceManager ranksCounterManager = applicationManager.getMapReduceManager(
      RANKS_COUNTER_PROGRAM.getEntityName());
    ranksCounterManager.start(Collections.singletonMap("system.resources.memory", "1024"));
    // wait 10 minutes for the mapreduce to execute
    ranksCounterManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    List<RunRecord> mrRanRecords =
      getRunRecords(1, programClient, RANKS_COUNTER_PROGRAM,
                    ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE);

    // mapreduce and spark should have 'COMPLETED' state because they complete on their own with a single run
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);


    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL,
                      SparkPageRankApp.SparkPageRankServiceHandler.TOTAL_PAGES_PATH + "/" + RANK);
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(TOTAL_PAGES, response.getResponseBodyAsString());

    url = new URL(serviceURL, SparkPageRankApp.SparkPageRankServiceHandler.RANKS_PATH);
    response = getRestClient().execute(HttpRequest.post(url).withBody("{\"url\":\"" + URL_1 + "\"}").build(),
                                       getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(RANK, response.getResponseBodyAsString());

    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

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
          new Relation(RANKS_DATASET, PAGE_RANK_SERVICE, AccessType.READ,
                       RunIds.fromString(serviceRanRecords.get(0).getPid())),
          new Relation(RANKS_COUNTS_DATASET, PAGE_RANK_SERVICE, AccessType.READ,
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

  private void testLineage(URL url, LineageRecord expected)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    LineageRecord lineageRecord = GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
    Assert.assertEquals(expected, lineageRecord);
  }
}
