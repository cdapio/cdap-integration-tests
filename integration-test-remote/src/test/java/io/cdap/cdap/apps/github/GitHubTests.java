/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.cdap.apps.github;

import com.google.common.base.Strings;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GitHubTests extends ETLTestBase {

  public static String TMP_TESTFILE = "/tmp/github_test_folder";
  private static String authorizationToken;
  private static String repoName;
  private static String repoOwner;
  private static String datasetName;

  protected static final ArtifactSelectorConfig GITHUB_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "github-plugin", "[0.0.0, 100.0.0)");

  @BeforeClass
  public static void setupTestClass() {
    // initialize api
    authorizationToken = System.getProperty("github.authorization.token");
    if (Strings.isNullOrEmpty(authorizationToken)) {
      throw new IllegalArgumentException("github.authorization.token system property must not be empty.");
    }
    repoName = System.getProperty("github.repo.name");
    if (Strings.isNullOrEmpty(repoName)) {
      throw new IllegalArgumentException("github.repo.name system property must not be empty.");
    }
    repoOwner = System.getProperty("github.repo.owner");
    if (Strings.isNullOrEmpty(repoOwner)) {
      throw new IllegalArgumentException("github.repo.owner system property must not be empty.");
    }
    datasetName = System.getProperty("github.repo.dataset");
    if (Strings.isNullOrEmpty(datasetName)) {
      throw new IllegalArgumentException("github.repo.dataset system property must not be empty.");
    }
  }

  @After
  public void removeFolders() throws IOException {
    FileUtils.deleteDirectory(new File(TMP_TESTFILE));
  }

  @Test(expected = IOException.class)
  public void testConfigFailed() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("authorizationToken", "invalid");
    sourceProps.put("repoName", "invalid");
    Map<String, String> sinkProp = getFileSinkProp();
    String batchSourceName = "GithubBatchSource";
    runPipeline(sourceProps, sinkProp, batchSourceName, ProgramRunStatus.FAILED);
  }

  @Test
  public void testReport() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    Map<String, String> sinkProp = getFileSinkProp();
    String batchSourceName = "GithubBatchSource";

    runPipeline(sourceProps, sinkProp, batchSourceName, ProgramRunStatus.COMPLETED);

    File file = new File(TMP_TESTFILE);
    Assert.assertTrue(file.isDirectory());
    File[] files = file.listFiles();
    Assert.assertEquals(1, files.length);
    File resultDir = files[0];
    Assert.assertTrue(resultDir.isDirectory());
    Assert.assertTrue(new File(resultDir, "_SUCCESS").exists());
  }

  private void runPipeline(Map<String, String> sourceProps, Map<String, String> sinkProp, String batchSourceName, ProgramRunStatus status) throws Exception {
    ETLStage source = new ETLStage(batchSourceName, new ETLPlugin(batchSourceName, BatchSource.PLUGIN_TYPE,
                                                                  sourceProps, GITHUB_ARTIFACT));

    ETLStage sink = new ETLStage("sink", new ETLPlugin("File", BatchSink.PLUGIN_TYPE, sinkProp));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("testapp");
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = applicationManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(status, 5, TimeUnit.MINUTES);
  }

  public Map<String, String> getFileSinkProp() {
    Map<String, String> properties = new HashMap<>();
    properties.put("suffix", "yyyy-MM-dd-HH-mm");
    properties.put("path", TMP_TESTFILE);
    properties.put("referenceName", "fileref");
    properties.put("format", "json");
    return properties;
  }

  public Map<String, String> getSourceMinimalDefaultConfigs() {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put("authorizationToken", authorizationToken);
    sourceProps.put("repoName", repoName);
    sourceProps.put("repoOwner", repoOwner);
    sourceProps.put("datasetName", datasetName);
    return sourceProps;
  }
}
