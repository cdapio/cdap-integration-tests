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

package co.cask.cdap.apps.purchase;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryBuilder;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MRTaskInfo;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class PurchaseTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(PurchaseTest.class);
  private static final Gson GSON = new Gson();

  @Test
  @Ignore // havn't tested this since quite a few changes...
  public void test() throws Exception {
    final Id.Program mrId = Id.Program.from(Constants.DEFAULT_NAMESPACE, PurchaseApp.APP_NAME, ProgramType.MAPREDUCE, PurchaseHistoryBuilder.class.getSimpleName());
    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    LOG.info("deployed");
    FlowManager purchaseFlow = applicationManager.startFlow("PurchaseFlow");
    LOG.info("startedFlow");
    getProgramClient().waitForStatus(mrId.getApplicationId(), ProgramType.FLOW, "PurchaseFlow", "RUNNING", 30, TimeUnit.SECONDS);
    LOG.info("waitForStatus");
    StreamWriter purchaseStream = applicationManager.getStreamWriter("purchaseStream");
    purchaseStream.send("John bought 5 apples for $10");
    purchaseStream.send("Jane bought 2 apples for $5");
    purchaseStream.send("Jane bought 1 banana for $1");
    LOG.info("Sent 3 events");
    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(3, 1, TimeUnit.MINUTES);
    LOG.info("flowlet done processing 3 events");

    MapReduceManager mapReduceManager = applicationManager.startMapReduce(mrId.getId());
    LOG.info("startMR");
    getProgramClient().waitForStatus(mrId.getApplicationId(), mrId.getType(), mrId.getId(), "RUNNING", 60, TimeUnit.SECONDS);
    LOG.info("MR - RUNNING");

    final Callable<List<RunRecord>> getRunRecordsCallable = new Callable<List<RunRecord>>() {
      @Override
      public List<RunRecord> call() throws Exception {
        LOG.info("gettingRuns");
        return getProgramClient().getProgramRuns(mrId.getApplicationId(), mrId.getType(), mrId.getId(), "RUNNING", 0, Long.MAX_VALUE, Integer.MAX_VALUE);
      }
    };
    // Run Record comes some time after program is registered to be in RUNNING state.
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getRunRecordsCallable.call().size();
      }
    }, 20, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    List<RunRecord> runningRecords = getRunRecordsCallable.call();
    Assert.assertEquals(1, runningRecords.size());
    RunRecord runRecord = runningRecords.get(0);
    Assert.assertEquals(ProgramRunStatus.RUNNING, runRecord.getStatus());
    String runId = runRecord.getPid();

    MRJobInfo info = waitForMRJobCompletion(mrId, runId);
    LOG.info("waitForMRJobSUCCEEDED");

    Long ingestedEvents = 3L;
    Long numCustomers = 2L;
    Assert.assertEquals(ingestedEvents, info.getCounters().get(TaskCounter.MAP_INPUT_RECORDS.name()));
    Assert.assertEquals(ingestedEvents, info.getCounters().get(TaskCounter.MAP_OUTPUT_RECORDS.name()));
    Assert.assertEquals(ingestedEvents, info.getCounters().get(TaskCounter.REDUCE_INPUT_RECORDS.name()));
    Assert.assertEquals(numCustomers, info.getCounters().get(TaskCounter.REDUCE_OUTPUT_RECORDS.name()));

    // The task-level metrics should total up to the job-level metrics.
    Long mapInputRecordsMetric = 0L;
    Long mapOutputRecordsMetric = 0L;
    Long reduceInputRecordsMetric = 0L;
    Long reduceOutputRecordsMetric = 0L;
    for (MRTaskInfo mapTask : info.getMapTasks()) {
      Map<String, Long> mapCounters = mapTask.getCounters();
      mapInputRecordsMetric += mapCounters.get(TaskCounter.MAP_INPUT_RECORDS.name());
      mapOutputRecordsMetric += mapCounters.get(TaskCounter.MAP_OUTPUT_RECORDS.name());
    }
    for (MRTaskInfo reduceTask : info.getReduceTasks()) {
      Map<String, Long> mapCounters = reduceTask.getCounters();
      reduceInputRecordsMetric += mapCounters.get(TaskCounter.REDUCE_INPUT_RECORDS.name());
      reduceOutputRecordsMetric += mapCounters.get(TaskCounter.REDUCE_OUTPUT_RECORDS.name());
    }
    Assert.assertEquals(ingestedEvents, mapInputRecordsMetric);
    Assert.assertEquals(ingestedEvents, mapOutputRecordsMetric);
    Assert.assertEquals(ingestedEvents, reduceInputRecordsMetric);
    Assert.assertEquals(numCustomers, reduceOutputRecordsMetric);

    //TODO: check equality with metrics?
//    Map<String, String> mrContext = forMR(mrId);
//    Map<String, String> runContext = addToContext(mrContext, ImmutableMap.of("run", runId));
//    MetricQueryResult query = new MetricsClient(getClientConfig()).query(runContext, "user.", null);

    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);
    LOG.info("waitForFinish");
  }

  // Blocks until MRJobInfo for the given MR run has completed successfully.
  private MRJobInfo waitForMRJobCompletion(Id.Program mrId, String runId) throws Exception {
    int attempt = 0;
    while (attempt++ < 120) {
      MRJobInfo infoWithRetry = getInfoWithRetry(mrId, runId);
      JobStatus.State state = JobStatus.State.valueOf(infoWithRetry.getState());
      LOG.info("state: " + state);
      if (JobStatus.State.SUCCEEDED == state) {
        return infoWithRetry;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new TimeoutException("Failed for MRJobInfo.getState() to be SUCCEEDED");
  }

  private MRJobInfo getInfoWithRetry(Id.Program mrId, String runId) throws Exception {
    // As soon as CDAP MapReduce program is running, the job isn't guaranteed to be submitted to Hadoop MR.
    // So, its possible that the information for the MapReduce job isn't found, and we need to retry.
    RESTClient restClient = new RESTClient(getClientConfig());
    String path = String.format("apps/%s/mapreduces/%s/runs/%s/info", mrId.getApplicationId(), mrId.getId(), runId);
    URL url = getClientConfig().resolveNamespacedURLV3(path);

    HttpResponse response = null;
    int attempt = 0;
    while (attempt++ < 20) {
      response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken(), 404);
      LOG.info("pinged /info");
      if (response.getResponseCode() == 200) {
        return GSON.fromJson(response.getResponseBodyAsString(), MRJobInfo.class);
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new TimeoutException(
      String.format("Failed to get info for MapReduce Program %s with run id %s. Actual response: %s",
                    mrId, runId, response));
  }

  private Map<String, String> forMR(Id.Program mrId) {
    return ImmutableMap.of("ns", mrId.getNamespaceId(), "app", mrId.getApplicationId(), "mr", mrId.getId());
  }

  private Map<String, String> addToContext(Map<String, String> context, Map<String, String> toAdd) {
    HashMap<String, String> newMap = Maps.newHashMap(context);
    newMap.putAll(toAdd);
    return newMap;
  }
}
