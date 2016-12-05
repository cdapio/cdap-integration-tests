/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SparkSink} and {@link SparkCompute} plugin type, using LogisticRegressionTrainer
 * and LogisticRegressionClassifier.
 */
@Category({
  // We don't support spark on these distros
  HDP20Incompatible.class,
  HDP21Incompatible.class,
  CDH51Incompatible.class,
  CDH52Incompatible.class,
  // this test is only compatible with spark v1.3.0 onwards and cdh5.3 uses spark v1.2.0
  CDH53Incompatible.class,
  // Currently, coopr doesn't provision MapR cluster with Spark
  MapR5Incompatible.class // MapR51 category is used for all MapR version
})
public class LogisticRegressionTest extends ETLTestBase {

  private static final String IMP_FIELD = "imp";
  private static final String READ_FIELD = "boolField";
  private static final String SPAM_PREDICTION_FIELD = "isSpam";

  private static final Schema SCHEMA =
    Schema.recordOf("simpleMessage",
                    Schema.Field.of(IMP_FIELD, Schema.of(Schema.Type.INT)),
                    Schema.Field.of(READ_FIELD, Schema.of(Schema.Type.INT)),
                    Schema.Field.of(SPAM_PREDICTION_FIELD, Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testSparkPlugins() throws Exception {
    // use the SparkSink to train a logistic regression model
    testSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSparkCompute();
  }

  private void testSparkSink() throws Exception {
    /*
     * stream --> sparksink
     */
    Map<String, String> sourceProp = ImmutableMap.of("name", "logisticRegressionTrainingStream",
                                                     "duration", "1d",
                                                     "format", "csv",
                                                     "schema", SCHEMA.toString());

    String fieldsToClassify = LogisticRegressionSpamMessage.IMP_FIELD + "," + LogisticRegressionSpamMessage.READ_FIELD;
    Map<String, String> sinkProp = ImmutableMap.of("fileSetName", "modelFileSet",
                                                   "path", "output",
                                                   "featureFieldsToInclude", fieldsToClassify,
                                                   "labelField", LogisticRegressionSpamMessage.SPAM_PREDICTION_FIELD,
                                                   "numClasses", "2");
    ETLPlugin sparkPlugin = new ETLPlugin("LogisticRegressionTrainer", SparkSink.PLUGIN_TYPE, sinkProp, null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, sourceProp, null)))
      .addStage(new ETLStage("customSink", sparkPlugin))
      .addConnection("source", "customSink")
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE_ENTITY.app("LogisticRegressionSpamTrainer");
    ApplicationManager appManager = deployApplication(appId, request);

    // set up five spam messages and five non-spam messages to be used for classification
    List<String> trainingMessages = ImmutableList.of("11,1,1.0",
                                                     "12,1,1.0",
                                                     "13,1,1.0",
                                                     "14,1,1.0",
                                                     "15,1,1.0",
                                                     "21,0,0.0",
                                                     "22,0,0.0",
                                                     "23,0,0.0",
                                                     "24,0,0.0",
                                                     "25,0,0.0");

    StreamId stream = TEST_NAMESPACE_ENTITY.stream("logisticRegressionTrainingStream");

    // write records to source
    StreamManager streamManager = getTestManager().getStreamManager(stream.toId());
    for (String spamMessage : trainingMessages) {
      streamManager.send(spamMessage);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }

  private void testSparkCompute() throws Exception {
    /**
     * stream --> sparkcompute --> tablesink
     */
    String textsToClassify = "textsToClassify";

    Schema simpleMessageSchema =
      Schema.recordOf("simpleMessage",
                      Schema.Field.of(LogisticRegressionSpamMessage.IMP_FIELD, Schema.of(Schema.Type.INT)),
                      Schema.Field.of(LogisticRegressionSpamMessage.READ_FIELD, Schema.of(Schema.Type.INT)));

    Map<String, String> sourceProp = ImmutableMap.of("name", textsToClassify,
                                                     "duration", "1d",
                                                     "format", "csv",
                                                     "schema", simpleMessageSchema.toString());

    String fieldsToClassify = LogisticRegressionSpamMessage.IMP_FIELD + "," + LogisticRegressionSpamMessage.READ_FIELD;

    Map<String, String> sparkProp = ImmutableMap.of("fileSetName", "modelFileSet",
                                                    "path", "output",
                                                    "featureFieldsToInclude", fieldsToClassify,
                                                    "predictionField",
                                                    LogisticRegressionSpamMessage.SPAM_PREDICTION_FIELD);

    ETLPlugin sparkPlugin = new ETLPlugin("LogisticRegressionClassifier", SparkCompute.PLUGIN_TYPE, sparkProp, null);

    Map<String, String> sinkProp = ImmutableMap.of("name", "classifiedTexts",
                                                   "schema", LogisticRegressionSpamMessage.SCHEMA.toString(),
                                                   "schema.row.field", LogisticRegressionSpamMessage.IMP_FIELD);

    ETLPlugin sinkPlugin = new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, sinkProp, null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, sourceProp, null)))
      .addStage(new ETLStage("sparkCompute", sparkPlugin))
      .addStage(new ETLStage("sink", sinkPlugin))
      .addConnection("source", "sparkCompute")
      .addConnection("sparkCompute", "sink")
      .build();

    ApplicationId app = TEST_NAMESPACE_ENTITY.app("SpamClassifier");
    ApplicationManager appManager = deployApplication(app, getBatchAppRequestV2(etlConfig));

    List<String> trainingMessages = ImmutableList.of("21,0",
                                                     "13,1",
                                                     "23,0");

    StreamId stream = TEST_NAMESPACE_ENTITY.stream(textsToClassify);

    // write records to source
    StreamManager streamManager = getTestManager().getStreamManager(stream.toId());
    for (String spamMessage : trainingMessages) {
      streamManager.send(spamMessage);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    Set<LogisticRegressionSpamMessage> expected = new HashSet<>();
    expected.add(new LogisticRegressionSpamMessage(21, 0, 0.0));
    expected.add(new LogisticRegressionSpamMessage(13, 1, 1.0));
    expected.add(new LogisticRegressionSpamMessage(23, 0, 0.0));

    Assert.assertEquals(expected, getClassifiedMessages());
  }

  private Set<LogisticRegressionSpamMessage> getClassifiedMessages() throws ExecutionException, InterruptedException {
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult exploreExecutionResult =
      queryClient.execute(TEST_NAMESPACE_ENTITY, "SELECT * FROM dataset_classifiedTexts").get();

    Set<LogisticRegressionSpamMessage> classifiedMessages = new HashSet<>();
    while (exploreExecutionResult.hasNext()) {
      List<Object> columns = exploreExecutionResult.next().getColumns();
      classifiedMessages.add(
        new LogisticRegressionSpamMessage((Integer) columns.get(1), (Integer) columns.get(2), (double) columns.get(0)));
    }
    return classifiedMessages;
  }
}
