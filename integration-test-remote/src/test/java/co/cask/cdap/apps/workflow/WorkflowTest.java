/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.apps.workflow;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.apps.wikipedia.TestData;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.wikipedia.SparkWikipediaClustering;
import co.cask.cdap.examples.wikipedia.TopNMapReduce;
import co.cask.cdap.examples.wikipedia.WikiContentValidatorAndNormalizer;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineApp;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineWorkflow;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH54Incompatible;
import co.cask.cdap.test.suite.category.HDP22Incompatible;
import co.cask.cdap.test.suite.category.HDP23Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.cdap.test.suite.category.RequiresSpark2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link WikipediaPipelineApp}.
 */
@Category({
  HDP22Incompatible.class,
  HDP23Incompatible.class,
  MapR5Incompatible.class,
  RequiresSpark2.class
})
public class WorkflowTest extends AudiTestBase {

  private static final ArtifactId ARTIFACT_ID = TEST_NAMESPACE.artifact("WikipediaPipelineArtifact", "1.0");
  private static final ApplicationId APP_ID = TEST_NAMESPACE.app(WikipediaPipelineApp.class.getSimpleName());
  private static final ArtifactSummary ARTIFACT_SUMMARY = new ArtifactSummary("WikipediaPipelineArtifact", "1.0");

  @Before
  public void setup() throws Exception {
    getTestManager().addAppArtifact(ARTIFACT_ID, WikipediaPipelineApp.class);
  }

  @Test
  @Category(CDH54Incompatible.class)
  public void testLDA() throws Exception {
    WikipediaPipelineApp.WikipediaAppConfig appConfig = new WikipediaPipelineApp.WikipediaAppConfig();
    AppRequest<WikipediaPipelineApp.WikipediaAppConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, appConfig);
    ApplicationManager appManager = deployApplication(APP_ID, appRequest);
    // Setup input streams with test data
    TestData.sendTestData(TEST_NAMESPACE, getTestManager());

    WorkflowManager workflowManager = appManager.getWorkflowManager(WikipediaPipelineWorkflow.class.getSimpleName());
    // Test with default threshold. Workflow should not proceed beyond first condition.
    testWorkflow(workflowManager, appConfig, 10);

    // Test with a reduced threshold, so the workflow proceeds beyond the first predicate
    testWorkflow(workflowManager, appConfig, 10, 1);
  }

  @Test
  public void testKMeans() throws Exception {
    // Test K-Means
    WikipediaPipelineApp.WikipediaAppConfig appConfig = new WikipediaPipelineApp.WikipediaAppConfig("kmeans");
    AppRequest<WikipediaPipelineApp.WikipediaAppConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, appConfig);
    ApplicationManager appManager = deployApplication(APP_ID, appRequest);
    // Setup input streams with test data
    TestData.sendTestData(TEST_NAMESPACE, getTestManager());
    WorkflowManager workflowManager = appManager.getWorkflowManager(WikipediaPipelineWorkflow.class.getSimpleName());
    testWorkflow(workflowManager, appConfig, 2, 1);
  }

  private void testWorkflow(WorkflowManager workflowManager,
                            WikipediaPipelineApp.WikipediaAppConfig config,
                            int expectedRecords) throws Exception {
    testWorkflow(workflowManager, config, expectedRecords, null);
  }

  private void testWorkflow(WorkflowManager workflowManager, WikipediaPipelineApp.WikipediaAppConfig config,
                            int expectedRecords,
                            @Nullable Integer threshold) throws Exception {
    // Wait for previous runs to finish
    List<RunRecord> history = workflowManager.getHistory();
    Map<String, String> args = new HashMap<>();
    args.put("system.resources.memory", "1024");
    if (threshold != null) {
      args.put("min.pages.threshold", String.valueOf(threshold));
    }
    workflowManager.start(args);

    // Wait for the current run to finish
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, history.size() + 1, 15, TimeUnit.MINUTES);
    // Wait for the workflow status. The timeout here is actually a sleep so, the timeout is a low value and instead
    // we retry a large number of times.
    workflowManager.waitForStatus(false, 60, 1);
    final String pid = getLatestPid(workflowManager.getHistory());
    Tasks.waitFor(true, () -> {
      try {
        WorkflowTokenNodeDetail tokenAtCondition =
                workflowManager.getTokenAtNode(pid, "EnoughDataToProceed", WorkflowToken.Scope.USER, "result");
        boolean conditionResult = Boolean.parseBoolean(tokenAtCondition.getTokenDataAtNode().get("result"));
        if (threshold == null) {
          Assert.assertFalse(conditionResult);
          assertWorkflowToken(workflowManager, config, pid, expectedRecords, false);
        } else {
          Assert.assertTrue(conditionResult);
          assertWorkflowToken(workflowManager, config, pid, expectedRecords, true);
        }
        return true;
      } catch (AssertionError | NotFoundException e) {
        // retry upon AssertionError or NotFoundException
        return false;
      }
    }, 30, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);

  }

  @Nullable
  private String getLatestPid(List<RunRecord> history) {
    String pid = null;
    long latestStartTime = 0;
    for (RunRecord runRecord : history) {
      // OK to use start ts, since we ensure that the next run begins after the previous run finishes in the test
      if (runRecord.getStartTs() > latestStartTime) {
        latestStartTime = runRecord.getStartTs();
        pid = runRecord.getPid();
      }
    }
    return pid;
  }

  private void assertWorkflowToken(WorkflowManager workflowManager,
                                   WikipediaPipelineApp.WikipediaAppConfig config, String pid,
                                   int expectedRecords,
                                   boolean continueConditionSucceeded) throws NotFoundException {
    assertTokenAtPageTitlesMRNode(workflowManager, pid);
    assertTokenAtRawDataMRNode(workflowManager, pid, continueConditionSucceeded);
    assertTokenAtNormalizationMRNode(workflowManager, pid, continueConditionSucceeded);
    assertTokenAtSparkClusteringNode(workflowManager, config, pid, expectedRecords, continueConditionSucceeded);
    assertTokenAtTopNMRNode(workflowManager, pid, continueConditionSucceeded);
  }

  private void assertTokenAtPageTitlesMRNode(WorkflowManager workflowManager, String pid) throws NotFoundException {
    WorkflowTokenNodeDetail pageTitlesUserTokens = workflowManager.getTokenAtNode(pid, "LikesToDataset", null, null);
    Assert.assertTrue(Boolean.parseBoolean(pageTitlesUserTokens.getTokenDataAtNode().get("result")));
    WorkflowTokenNodeDetail pageTitlesSystemTokens =
      workflowManager.getTokenAtNode(pid, "LikesToDataset", WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(2, Integer.parseInt(pageTitlesSystemTokens.getTokenDataAtNode().get("custom.num.records")));
  }

  private void assertTokenAtRawDataMRNode(WorkflowManager workflowManager, String pid,
                                          boolean continueConditionSucceeded) throws NotFoundException {
    if (!continueConditionSucceeded) {
      return;
    }
    WorkflowTokenNodeDetail rawWikiDataUserTokens =
      workflowManager.getTokenAtNode(pid, "WikiDataToDataset", null, null);
    Assert.assertTrue(Boolean.parseBoolean(rawWikiDataUserTokens.getTokenDataAtNode().get("result")));
    WorkflowTokenNodeDetail rawWikiDataSystemTokens =
      workflowManager.getTokenAtNode(pid, "WikiDataToDataset", WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(2, Integer.parseInt(rawWikiDataSystemTokens.getTokenDataAtNode().get("custom.num.records")));
  }

  private void assertTokenAtNormalizationMRNode(WorkflowManager workflowManager, String pid,
                                                boolean continueConditionSucceeded) throws NotFoundException {
    if (!continueConditionSucceeded) {
      return;
    }
    WorkflowTokenNodeDetail normalizedDataUserTokens =
      workflowManager.getTokenAtNode(pid, WikiContentValidatorAndNormalizer.NAME, null, null);
    Assert.assertTrue(Boolean.parseBoolean(normalizedDataUserTokens.getTokenDataAtNode().get("result")));
    WorkflowTokenNodeDetail normalizedDataSystemTokens =
      workflowManager.getTokenAtNode(pid, WikiContentValidatorAndNormalizer.NAME, WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(2, Integer.parseInt(normalizedDataSystemTokens.getTokenDataAtNode().get("custom.num.records")));
  }

  private void assertTokenAtSparkClusteringNode(WorkflowManager workflowManager,
                                                WikipediaPipelineApp.WikipediaAppConfig config, String pid,
                                                int expectedRecords,
                                                boolean continueConditionSucceeded) throws NotFoundException {
    if (!continueConditionSucceeded) {
      return;
    }
    @SuppressWarnings("ConstantConditions")
    String sparkProgramName = SparkWikipediaClustering.NAME + "-" + config.clusteringAlgorithm.toUpperCase();
    WorkflowTokenNodeDetail ldaUserTokens =
      workflowManager.getTokenAtNode(pid, sparkProgramName, null, null);
    Assert.assertEquals(expectedRecords, Integer.parseInt(ldaUserTokens.getTokenDataAtNode().get("num.records")));
    Assert.assertTrue(ldaUserTokens.getTokenDataAtNode().containsKey("highest.score.term"));
    Assert.assertTrue(ldaUserTokens.getTokenDataAtNode().containsKey("highest.score.value"));
    WorkflowTokenNodeDetail ldaSystemTokens =
      workflowManager.getTokenAtNode(pid, sparkProgramName, WorkflowToken.Scope.SYSTEM, null);
    Assert.assertTrue(ldaSystemTokens.getTokenDataAtNode().isEmpty());
  }

  private void assertTokenAtTopNMRNode(WorkflowManager workflowManager, String pid,
                                       boolean continueConditionSucceeded) throws NotFoundException {
    if (!continueConditionSucceeded) {
      return;
    }
    WorkflowTokenNodeDetail topNUserTokens = workflowManager.getTokenAtNode(pid, TopNMapReduce.NAME, null, null);
    Assert.assertTrue(Boolean.parseBoolean(topNUserTokens.getTokenDataAtNode().get("result")));
    WorkflowTokenNodeDetail topNSystemTokens =
      workflowManager.getTokenAtNode(pid, TopNMapReduce.NAME, WorkflowToken.Scope.SYSTEM, null);
    Assert.assertEquals(10, Integer.parseInt(topNSystemTokens.getTokenDataAtNode().get("custom.num.records")));
  }
}
