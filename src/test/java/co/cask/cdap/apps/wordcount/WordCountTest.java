/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.apps.wordcount;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.StreamManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * Test for {@link WordCount}.
 */
public class WordCountTest extends AudiTestBase {
  private static final String[] wordEvents = {"hello world", "good morning"};

  @Test
  @Ignore // undo once finalized
  public void test() throws Exception {
    /**
     * 1) Start flow.
     * 2) Send Event.
     * 3) Get Runid, and check the metrics for the flow run.
     * 4) Stop flow.
     * 5) Repeat the steps 1 to 4
     * 6) check metrics between runs for aggregate.
     */
    ApplicationManager applicationManager = deployApplication(WordCount.class);
    int numWordsProcessed = 0;
    int longestWordLengthAcrossRuns = Integer.MIN_VALUE;

    ApplicationClient appClient = new ApplicationClient(getClientConfig(), getRestClient());
    MetricsClient metricsClient = new MetricsClient(getClientConfig());
    ProgramClient programClient = new ProgramClient(getClientConfig(), getRestClient(),
                                                    appClient);

    Id.Program flowId = Id.Program.from(Constants.DEFAULT_NAMESPACE, "WordCount", ProgramType.FLOW, "WordCounter");
    Map<String, String> flowContext = forFlow(flowId);
    String longestWordLengthMetric = "user.longest.word.length";

    List<ApplicationRecord> deployedApps = appClient.list();
    Assert.assertEquals(1, deployedApps.size());
    Assert.assertEquals("WordCount", deployedApps.get(0).getName());

    for (int i = 0; i < wordEvents.length; i++) {
      FlowManager flowManager = applicationManager.startFlow("WordCounter");
      programClient.waitForStatus("WordCount", ProgramType.FLOW, "WordCounter", "RUNNING", 30, TimeUnit.SECONDS);
      List<RunRecord> runningRecords = programClient.getProgramRuns(flowId.getApplicationId(), flowId.getType(), flowId.getId(), "RUNNING", 0, Long.MAX_VALUE, Integer.MAX_VALUE);
      //There should only be one run that is running
      Assert.assertEquals(1, runningRecords.size());
      RunRecord runRecord = runningRecords.get(0);
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecord.getStatus());
      String runId = runRecord.getPid();

      StreamManager wordStream = getTestManager().getStreamManager(Id.Stream.from(Constants.DEFAULT_NAMESPACE, "wordStream"));
      wordStream.send(wordEvents[i]);
      // make sure we processed the sent event, before starting the workflow
      numWordsProcessed += wordEvents[i].split(" ").length;

      // try out metricsClient#getFlowletMetrics()
      RuntimeMetrics counterMetrics = flowManager.getFlowletMetrics("counter");
      counterMetrics.waitForProcessed(numWordsProcessed, 1, TimeUnit.MINUTES);

      int longestWordLength = longestString(wordEvents[i].split(" ")).length();
      longestWordLengthAcrossRuns = Math.max(longestWordLengthAcrossRuns, longestWordLength);
      TimeUnit.SECONDS.sleep(1);
      checkMetrics(metricsClient, addToContext(flowContext, ImmutableMap.of("run", runId)),
                   longestWordLengthMetric, longestWordLength);

      flowManager.stop();
      programClient.waitForStatus("WordCount", ProgramType.FLOW, "WordCounter", "STOPPED", 30, TimeUnit.SECONDS);
    }

    // Check user metrics sum aggregated across runs
    checkMetrics(metricsClient, flowContext, longestWordLengthMetric, longestWordLengthAcrossRuns);
  }

  private Map<String, String> forFlow(Id.Program flowId) {
    return ImmutableMap.of("ns", flowId.getNamespaceId(), "app", flowId.getApplicationId(), "fl", flowId.getId());
  }

  private Map<String, String> addToContext(Map<String, String> context, Map<String, String> toAdd) {
    HashMap<String, String> newMap = Maps.newHashMap(context);
    newMap.putAll(toAdd);
    return newMap;
  }

  private void checkMetrics(MetricsClient metricsClient, Map<String, String> context, String metric, final int expectedValue) throws Exception {
    final MetricQueryResult metricResponse = metricsClient.query(metric, context);
    // allow retries for 10 seconds, with a sleep of 1 second between retries
    callWithRetry(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        checkMetricResult(metricResponse, expectedValue);
        return null;
      }
    }, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }

  private void checkMetricResult(MetricQueryResult metricQueryResult, int expectedValue) {
    MetricQueryResult.TimeSeries[] series = metricQueryResult.getSeries();
    if (series.length != 1) {
      Assert.fail("Series length: " + series.length);
    }
    MetricQueryResult.TimeValue[] data = series[0].getData();
    if (data.length != 1) {
      Assert.fail("Data length: " + data.length);
    }
    long actualValue = data[0].getValue();
    Assert.assertEquals(expectedValue, actualValue);
  }

  private String longestString(String[] words) {
    Preconditions.checkArgument(words.length > 0);
    String longestSoFar = words[0];
    for (String word : words) {
      if (word.length() > longestSoFar.length()) {
        longestSoFar = word;
      }
    }
    return longestSoFar;
  }

  // Copied from: https://github.com/caskdata/cdap/blob/4b34f3a3ad9a6687e5b3edd60cca828648ff180f/cdap-examples/StreamConversion/src/test/java/co/cask/cdap/examples/streamconversion/StreamConversionTest.java#L130
  // Can be removed from here, if copied to Tasks.java
  private <T> T callWithRetry(final Callable<T> callable, long timeout, TimeUnit timeoutUnit,
                              long sleepDelay, TimeUnit sleepDelayUnit)
    throws InterruptedException, ExecutionException, TimeoutException {

    final AtomicMarkableReference<T> result = new AtomicMarkableReference<>(null, false);
    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        try {
          result.set(callable.call(), true);
        } catch (AssertionError e) {
          // retry
          return false;
        }
        return true;
      }
    }, timeout, timeoutUnit, sleepDelay, sleepDelayUnit);
    Assert.assertTrue(result.isMarked());
    return result.getReference();
  }
}
