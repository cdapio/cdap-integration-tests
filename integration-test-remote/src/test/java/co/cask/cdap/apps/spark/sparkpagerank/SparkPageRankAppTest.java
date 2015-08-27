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

package co.cask.cdap.apps.spark.sparkpagerank;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.sparkpagerank.SparkPageRankApp;
import co.cask.cdap.examples.sparkpagerank.SparkPageRankProgram;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of {@link SparkPageRankApp}
 */
public class SparkPageRankAppTest extends AudiTestBase {

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


  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    final ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(SparkPageRankApp.class);

    // none of the programs should have any run records
    assertRuns(0, programClient, null, PAGE_RANK_SERVICE, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);

    StreamManager backlinkURLStream = getTestManager().getStreamManager(STREAM);
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_2));
    backlinkURLStream.send(Joiner.on(" ").join(URL_1, URL_3));
    backlinkURLStream.send(Joiner.on(" ").join(URL_2, URL_1));
    backlinkURLStream.send(Joiner.on(" ").join(URL_3, URL_1));

    // Start service
    ServiceManager serviceManager = applicationManager.getServiceManager(PAGE_RANK_SERVICE.getId()).start();
    serviceManager.waitForStatus(true, 60, 1);
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
    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    pageRankManager.waitForStatus(false, 10 * 60, 1);


    // Start mapreduce and await completion
    MapReduceManager ranksCounterManager = applicationManager.getMapReduceManager(RANKS_COUNTER_PROGRAM.getId());
    ranksCounterManager.start();
    ranksCounterManager.waitForStatus(true, 60, 1);
    ranksCounterManager.waitForStatus(false, 10 * 60, 1);

    // mapreduce and spark should have 'COMPLETED' state because they complete on their own with a single run
    assertRuns(1, programClient, ProgramRunStatus.COMPLETED, RANKS_COUNTER_PROGRAM, PAGE_RANK_PROGRAM);

    URL url = new URL(serviceManager.getServiceURL(),
                      SparkPageRankApp.SparkPageRankServiceHandler.TOTAL_PAGES_PATH + "/" + RANK);
    HttpResponse response = retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(url).build(),
                                           getRestClient(), 120, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(TOTAL_PAGES, response.getResponseBodyAsString());

    url = new URL(serviceManager.getServiceURL(), SparkPageRankApp.SparkPageRankServiceHandler.RANKS_PATH);
    response = retryRestCalls(HttpURLConnection.HTTP_OK,
                              HttpRequest.post(url).withBody("{\"url\":\"" + URL_1 + "\"}").build(),
                              getRestClient(), 120, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(RANK, response.getResponseBodyAsString());

    serviceManager.stop();
    serviceManager.waitForStatus(false, 60, 1);


    // services should 'KILLED' state because they were explicitly stopped with a single run
    assertRuns(1, programClient, ProgramRunStatus.KILLED, PAGE_RANK_SERVICE);
  }
}
