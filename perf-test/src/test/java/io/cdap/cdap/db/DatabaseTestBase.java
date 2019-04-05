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

package io.cdap.cdap.db;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;

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
      ImmutableMap.of(io.cdap.cdap.common.conf.Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                      io.cdap.cdap.common.conf.Constants.Metrics.Tag.APP, appId.getEntityName());

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
