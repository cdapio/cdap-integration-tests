package io.cdap.cdap.apps.ga360;

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

public class GoogleAnalyticsTests extends ETLTestBase {

  public static String TMP_TESTFILE = "/tmp/ga360_test_folder";
  private static String authorizationToken;
  private static String viewId;
  private static String startDate;
  private static String endDate;
  private static String metricsList;

  protected static final ArtifactSelectorConfig GOOGLE_ANALYTICS_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-analytics-360-plugin", "[0.0.0, 100.0.0)");

  @BeforeClass
  public static void setupTestClass() {
    // initialize api
    authorizationToken = System.getProperty("ga.authorization.token");
    if (Strings.isNullOrEmpty(authorizationToken)) {
      throw new IllegalArgumentException("ga.authorization.token system property must not be empty.");
    }
    viewId = System.getProperty("ga.view.id");
    if (Strings.isNullOrEmpty(viewId)) {
      throw new IllegalArgumentException("ga.view.id system property must not be empty.");
    }
    startDate = System.getProperty("ga.date.start");
    if (Strings.isNullOrEmpty(startDate)) {
      throw new IllegalArgumentException("ga.date.start system property must not be empty.");
    }
    endDate = System.getProperty("ga.date.end");
    if (Strings.isNullOrEmpty(endDate)) {
      throw new IllegalArgumentException("ga.date.end system property must not be empty.");
    }
    metricsList = System.getProperty("ga.metrics");
    if (Strings.isNullOrEmpty(metricsList)) {
      throw new IllegalArgumentException("ga.metrics system property must not be empty.");
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
    sourceProps.put("viewId", "invalid");
    Map<String, String> sinkProp = getFileSinkProp();
    String batchSourceName = "GoogleAnalyticsBatchSource";
    runPipeline(sourceProps, sinkProp, batchSourceName, ProgramRunStatus.FAILED);
  }

  @Test
  public void testReport() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    Map<String, String> sinkProp = getFileSinkProp();
    String batchSourceName = "GoogleAnalyticsBatchSource";

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
                                                                  sourceProps, GOOGLE_ANALYTICS_ARTIFACT));

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
    sourceProps.put("viewId", viewId);
    sourceProps.put("startDate", startDate);
    sourceProps.put("endDate", endDate);
    sourceProps.put("metricsList", metricsList);
    return sourceProps;
  }
}
