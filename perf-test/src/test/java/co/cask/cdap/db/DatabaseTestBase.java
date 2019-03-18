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

package co.cask.cdap.db;

import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for testing measuring database source and sink.
 */
public abstract class DatabaseTestBase extends ETLTestBase {

  protected PrintStream out;

  public void writeMetrics(Map<String, String> tags) throws Exception {

    String property = System.getProperty("database.metrics.file");
    out = property == null ? System.out : new PrintStream(new FileOutputStream(property, false));

    StringBuilder metrics = new StringBuilder();
    List<String> metricsList = getMetricsClient().searchMetrics(tags);

    for (String metric : metricsList) {
      metrics.append(metric);
      metrics.append("=");
      metrics.append(getMetricValue(tags, metric));
      metrics.append("\n");
    }

    out.println(metrics.toString());
  }

  public void testRun() throws Exception {

    ETLBatchConfig batchConfig = createETLBatchConfig(createSource(), createSink());

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(batchConfig);
    ApplicationId appId = TEST_NAMESPACE.app("DBTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 6, TimeUnit.MINUTES);

    Map<String, String> tags =
      ImmutableMap.of(co.cask.cdap.common.conf.Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                      co.cask.cdap.common.conf.Constants.Metrics.Tag.APP, appId.getEntityName());

    writeMetrics(tags);
  }

  protected ETLBatchConfig createETLBatchConfig(ETLStage source, ETLStage sink) {
    return ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();
  }

  protected abstract ETLStage createSource();

  protected abstract ETLStage createSink();
}
