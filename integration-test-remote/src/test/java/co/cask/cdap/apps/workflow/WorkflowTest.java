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

package co.cask.cdap.apps.workflow;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.wikipedia.SparkWikipediaClustering;
import co.cask.cdap.examples.wikipedia.TopNMapReduce;
import co.cask.cdap.examples.wikipedia.WikiContentValidatorAndNormalizer;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineApp;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineWorkflow;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.CDH54Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.HDP22Incompatible;
import co.cask.cdap.test.suite.category.HDP23Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link WikipediaPipelineApp}.
 */
@Category({
  HDP20Incompatible.class,
  HDP21Incompatible.class,
  HDP22Incompatible.class,
  HDP23Incompatible.class,
  CDH51Incompatible.class,
  CDH52Incompatible.class,
  CDH53Incompatible.class,
  MapR5Incompatible.class
})
public class WorkflowTest extends AudiTestBase {

  private static final Id.Artifact ARTIFACT_ID =
    Id.Artifact.from(Id.Namespace.DEFAULT, "WikipediaPipelineArtifact", new ArtifactVersion("1.0"));
  private static final Id.Application APP_ID =
    Id.Application.from(Id.Namespace.DEFAULT, WikipediaPipelineApp.class.getSimpleName());
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
    createTestData();

    WorkflowManager workflowManager = appManager.getWorkflowManager(WikipediaPipelineWorkflow.class.getSimpleName());
    // Test with default threshold. Workflow should not proceed beyond first condition.
    testWorkflow(workflowManager, appConfig);

    // Test with a reduced threshold, so the workflow proceeds beyond the first predicate
    testWorkflow(workflowManager, appConfig, 1);
  }

  @Test
  public void testKMeans() throws Exception {
    // Test K-Means
    WikipediaPipelineApp.WikipediaAppConfig appConfig = new WikipediaPipelineApp.WikipediaAppConfig("kmeans");
    AppRequest<WikipediaPipelineApp.WikipediaAppConfig> appRequest = new AppRequest<>(ARTIFACT_SUMMARY, appConfig);
    ApplicationManager appManager = deployApplication(APP_ID, appRequest);
    // Setup input streams with test data
    createTestData();
    WorkflowManager workflowManager = appManager.getWorkflowManager(WikipediaPipelineWorkflow.class.getSimpleName());
    testWorkflow(workflowManager, appConfig, 1);
  }

  private void createTestData() throws Exception {
    Id.Stream likesStream = Id.Stream.from(Id.Namespace.DEFAULT, "pageTitleStream");
    StreamManager likesStreamManager = getTestManager().getStreamManager(likesStream);
    String like1 = "{\"name\":\"Metallica\",\"id\":\"107926539230502\",\"created_time\":\"2015-06-25T17:14:47+0000\"}";
    String like2 = "{\"name\":\"grunge\",\"id\":\"911679552186992\",\"created_time\":\"2015-07-20T17:37:04+0000\"}";
    likesStreamManager.send(like1);
    likesStreamManager.send(like2);
    Id.Stream rawWikiDataStream = Id.Stream.from(Id.Namespace.DEFAULT, "wikiStream");
    StreamManager rawWikipediaStreamManager = getTestManager().getStreamManager(rawWikiDataStream);
    String data1 = "{\"batchcomplete\":\"\",\"query\":{\"normalized\":[{\"from\":\"metallica\",\"to\":\"Metallica\"}]" +
      ",\"pages\":{\"18787\":{\"pageid\":18787,\"ns\":0,\"title\":\"Metallica\",\"revisions\":[{\"contentformat\":" +
      "\"text/x-wiki\",\"contentmodel\":\"wikitext\",\"*\":\"{{Other uses}}{{pp-semi|small=yes}}{{pp-move-indef|" +
      "small=yes}}{{Use mdy dates|date=April 2013}}{{Infobox musical artist|name = Metallica|image = Metallica at " +
      "The O2 Arena London 2008.jpg|caption = Metallica in [[London]] in 2008. From left to right: [[Kirk Hammett]], " +
      "[[Lars Ulrich]], [[James Hetfield]] and [[Robert Trujillo]]\"}]}}}}";
    String data2 = "{\"batchcomplete\":\"\",\"query\":{\"pages\":{\"51580\":{\"pageid\":51580,\"ns\":0," +
      "\"title\":\"Grunge\",\"revisions\":[{\"contentformat\":\"text/x-wiki\",\"contentmodel\":\"wikitext\"," +
      "\"*\":\"{{About|the music genre}}{{Infobox music genre| name  = Grunge| bgcolor = crimson| color = white| " +
      "stylistic_origins = {{nowrap|[[Alternative rock]], [[hardcore punk]],}} [[Heavy metal music|heavy metal]], " +
      "[[punk rock]], [[hard rock]], [[noise rock]]| cultural_origins  = Mid-1980s, [[Seattle|Seattle, Washington]], " +
      "[[United States]]| instruments = [[Electric guitar]], [[bass guitar]], [[Drum kit|drums]], " +
      "[[Singing|vocals]]| derivatives = [[Post-grunge]], [[nu metal]]| subgenrelist = | subgenres = | fusiongenres" +
      "      = | regional_scenes   = [[Music of Washington (state)|Washington state]]| other_topics      = * " +
      "[[Alternative metal]]* [[Generation X]]* [[Grunge speak|grunge speak hoax]]* [[timeline of alternative " +
      "rock]]}}'''Grunge''' (sometimes referred to as the '''Seattle sound''') is a subgenre of [[alternative rock]]" +
      " that emerged during the mid-1980s in the American state of [[Washington (state)|Washington]], particularly " +
      "in [[Seattle]].  The early grunge movement revolved around Seattle's [[independent record label]] " +
      "[[Sub Pop]], but by the early 1990s its popularity had spread, with grunge acts in California and other " +
      "parts of the U.S. building strong followings and signing major record deals.Grunge became commercially " +
      "successful in the first half of the 1990s, due mainly to the release of [[Nirvana (band)|Nirvana]]'s " +
      "''[[Nevermind]]'', [[Pearl Jam]]'s ''[[Ten (Pearl Jam album)|Ten]]'', [[Soundgarden]]'s " +
      "''[[Badmotorfinger]]'', [[Alice in Chains]]' ''[[Dirt (Alice in Chains album)|Dirt]]'', and " +
      "[[Stone Temple Pilots]]' ''[[Core (Stone Temple Pilots album)|Core]]''.\"}]}}}}";
    rawWikipediaStreamManager.send(data1);
    rawWikipediaStreamManager.send(data2);

    waitForStreamToBePopulated(likesStreamManager, 2);
    waitForStreamToBePopulated(rawWikipediaStreamManager, 2);
  }

  private void waitForStreamToBePopulated(final StreamManager streamManager, int numEvents) throws Exception {
    Tasks.waitFor(numEvents, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        List<StreamEvent> streamEvents = streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE);
        return streamEvents.size();
      }
    }, 10, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }

  private void testWorkflow(WorkflowManager workflowManager,
                            WikipediaPipelineApp.WikipediaAppConfig config) throws Exception {
    testWorkflow(workflowManager, config, null);
  }

  private void testWorkflow(WorkflowManager workflowManager, WikipediaPipelineApp.WikipediaAppConfig config,
                            @Nullable Integer threshold) throws Exception {
    if (threshold == null) {
      workflowManager.start();
    } else {
      workflowManager.start(ImmutableMap.of("min.pages.threshold", String.valueOf(threshold)));
    }
    workflowManager.waitForFinish(15, TimeUnit.MINUTES);
    String pid = getLatestPid(workflowManager.getHistory());
    WorkflowTokenNodeDetail tokenAtCondition =
      workflowManager.getTokenAtNode(pid, "EnoughDataToProceed", WorkflowToken.Scope.USER, "result");
    boolean conditionResult = Boolean.parseBoolean(tokenAtCondition.getTokenDataAtNode().get("result"));
    if (threshold == null) {
      Assert.assertFalse(conditionResult);
      assertWorkflowToken(workflowManager, config, pid, false);
    } else {
      Assert.assertTrue(conditionResult);
      assertWorkflowToken(workflowManager, config, pid, true);
    }
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
                                   boolean continueConditionSucceeded) throws NotFoundException {
    assertTokenAtPageTitlesMRNode(workflowManager, pid);
    assertTokenAtRawDataMRNode(workflowManager, pid, continueConditionSucceeded);
    assertTokenAtNormalizationMRNode(workflowManager, pid, continueConditionSucceeded);
    assertTokenAtSparkClusteringNode(workflowManager, config, pid, continueConditionSucceeded);
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
                                                boolean continueConditionSucceeded) throws NotFoundException {
    if (!continueConditionSucceeded) {
      return;
    }
    @SuppressWarnings("ConstantConditions")
    String sparkProgramName = SparkWikipediaClustering.NAME + "-" + config.clusteringAlgorithm.toUpperCase();
    WorkflowTokenNodeDetail ldaUserTokens =
      workflowManager.getTokenAtNode(pid, sparkProgramName, null, null);
    Assert.assertEquals(10, Integer.parseInt(ldaUserTokens.getTokenDataAtNode().get("num.records")));
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
