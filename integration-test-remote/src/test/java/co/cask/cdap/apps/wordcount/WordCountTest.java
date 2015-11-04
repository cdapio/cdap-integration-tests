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
package co.cask.cdap.apps.wordcount;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.examples.wordcount.WordCount;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.StreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link WordCount}.
 */
public class WordCountTest extends AudiTestBase {
  private static final String[] wordEvents = {"hello world", "good morning"};

  @Test
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
    int longestWordLengthAcrossRuns = 0;

    ApplicationClient appClient = getApplicationClient();
    ProgramClient programClient = getProgramClient();

    Id.Program flowId = Id.Program.from(TEST_NAMESPACE, "WordCount", ProgramType.FLOW, "WordCounter");
    Map<String, String> flowTags = getFlowTags(flowId);
    String longestWordLengthMetric = "user.longest.word.length";

    List<ApplicationRecord> deployedApps = appClient.list();
    Assert.assertEquals(1, deployedApps.size());
    Assert.assertEquals("WordCount", deployedApps.get(0).getName());

    for (int i = 0; i < wordEvents.length; i++) {
      FlowManager flowManager = applicationManager.startFlow("WordCounter");
      // TODO: make an AbstractRemoteProgramManager which has default timeouts
      getProgramClient().waitForStatus(flowId.getApplicationId(), flowId.getType(), flowId.getId(),
                                       "RUNNING", 30, TimeUnit.SECONDS);
      List<RunRecord> runningRecords =
        programClient.getProgramRuns(flowId.getApplicationId(), flowId.getType(), flowId.getId(),
                                     ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
      //There should only be one run that is running
      Assert.assertEquals(1, runningRecords.size());
      RunRecord runRecord = runningRecords.get(0);
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecord.getStatus());
      String runId = runRecord.getPid();

      StreamWriter wordStream = applicationManager.getStreamWriter("wordStream");
      wordStream.send(wordEvents[i]);
      // make sure we processed the sent event, before starting the workflow
      numWordsProcessed += wordEvents[i].split(" ").length;

      // try out metricsClient#getFlowletMetrics()
      RuntimeMetrics counterMetrics = flowManager.getFlowletMetrics("counter");
      counterMetrics.waitForProcessed(numWordsProcessed, 1, TimeUnit.MINUTES);

      int longestWordLength = longestString(wordEvents[i].split(" ")).length();
      longestWordLengthAcrossRuns += longestWordLength;
      TimeUnit.SECONDS.sleep(1);
      checkMetric(addToTags(flowTags, ImmutableMap.of("run", runId)), longestWordLengthMetric, longestWordLength, 10);

      flowManager.stop();
      getProgramClient().waitForStatus(flowId.getApplicationId(), flowId.getType(), flowId.getId(),
                                       "STOPPED", 30, TimeUnit.SECONDS);
    }

    // Check user metrics sum aggregated across runs
    checkMetric(flowTags, longestWordLengthMetric, longestWordLengthAcrossRuns, 30);
  }

  private Map<String, String> getFlowTags(Id.Program flowId) {
    return ImmutableMap.of("ns", flowId.getNamespaceId(), "app", flowId.getApplicationId(), "fl", flowId.getId(),
                           "run", "*");
  }

  private Map<String, String> addToTags(Map<String, String> tags, Map<String, String> toAdd) {
    HashMap<String, String> newTags = Maps.newHashMap(tags);
    newTags.putAll(toAdd);
    return newTags;
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

}
