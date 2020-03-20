/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.apps.spark;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageSerializer;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.suite.category.RequiresSpark2;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
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
  RequiresSpark2.class
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
  // We ingest data into the dataset through an app created through integration test infrastructure
  private static final ApplicationId INPUT_DATASET_APP = TEST_NAMESPACE.app(SparkPageRankApp.BACKLINK_URL_DATASET);
  private static final ProgramId PAGE_RANK_SERVICE = SPARK_PAGE_RANK_APP.service(SparkPageRankApp.SERVICE_HANDLERS);
  private static final ProgramId RANKS_COUNTER_PROGRAM = SPARK_PAGE_RANK_APP.mr(
    SparkPageRankApp.RanksCounter.class.getSimpleName());
  private static final ProgramId PAGE_RANK_PROGRAM = SPARK_PAGE_RANK_APP.spark(
    SparkPageRankApp.PageRankSpark.class.getSimpleName());
  private static final ProgramId DATASET_SERVICE = INPUT_DATASET_APP.service("DatasetService");
  private static final DatasetId INPUT_DATASET = TEST_NAMESPACE.dataset(SparkPageRankApp.BACKLINK_URL_DATASET);
  private static final DatasetId RANKS_DATASET = TEST_NAMESPACE.dataset("ranks");
  private static final DatasetId RANKS_COUNTS_DATASET = TEST_NAMESPACE.dataset("rankscount");

  @Test
  public void test() throws Exception {
    final ProgramClient programClient = getProgramClient();

    long startTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    ApplicationManager applicationManager = deployApplication(SparkPageRankApp.class);

    // none of the programs should have any run records
    assertRuns(0, programClient, ProgramRunStatus.ALL, PAGE_RANK_SERVICE, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);

    ingestData();

    // Start service
    ServiceManager serviceManager = applicationManager.getServiceManager(PAGE_RANK_SERVICE.getEntityName());
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    // Start Spark Page Rank and await completion
    SparkManager pageRankManager = applicationManager.getSparkManager(PAGE_RANK_PROGRAM.getEntityName());
    Map<String, String> runtimeArgs = ImmutableMap.of("task.client.system.resources.memory", "1024");
    startAndWaitForRun(pageRankManager, ProgramRunStatus.COMPLETED, runtimeArgs, 10, TimeUnit.MINUTES);

    List<RunRecord> sparkRanRecords =
      getRunRecords(1, programClient, PAGE_RANK_PROGRAM,
                    ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE);

    // Start mapreduce and await completion
    MapReduceManager ranksCounterManager = applicationManager.getMapReduceManager(
      RANKS_COUNTER_PROGRAM.getEntityName());
    // wait 10 minutes for the mapreduce to execute
    startAndWaitForRun(ranksCounterManager, ProgramRunStatus.COMPLETED,
                       Collections.singletonMap("system.resources.memory", "1024"), 10, TimeUnit.MINUTES);

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
    serviceManager.waitForRuns(ProgramRunStatus.KILLED, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS,
                               POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);

    List<RunRecord> serviceRanRecords =
      getRunRecords(1, programClient, PAGE_RANK_SERVICE,
                    ProgramRunStatus.KILLED.name(), 0, Long.MAX_VALUE);

    List<RunRecord> inputRunRecords =
      getRunRecords(1, programClient, DATASET_SERVICE,
                    ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE);

    long endTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 100;

    LineageRecord expected =
      // When CDAP-3657 is fixed, we will no longer need to use LineageSerializer for serializing.
      // Instead we can direclty use Id.toString() to get the program and data keys.
      LineageSerializer.toLineageRecord(
        startTimeSecs,
        endTimeSecs,
        new Lineage(ImmutableSet.of(
          new Relation(INPUT_DATASET, DATASET_SERVICE, AccessType.WRITE,
                       RunIds.fromString(inputRunRecords.get(0).getPid())),
          new Relation(INPUT_DATASET, PAGE_RANK_PROGRAM, AccessType.READ,
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

  protected void ingestData() throws Exception {
    // write input data
    DataSetManager<KeyValueTable> datasetManager = getKVTableDataset(SparkPageRankApp.BACKLINK_URL_DATASET);
    KeyValueTable table = datasetManager.get();
    table.write("1", Joiner.on(" ").join(URL_1, URL_2));
    table.write("2", Joiner.on(" ").join(URL_1, URL_3));
    table.write("3", Joiner.on(" ").join(URL_2, URL_1));
    table.write("4", Joiner.on(" ").join(URL_3, URL_1));
    datasetManager.flush();
  }
}
