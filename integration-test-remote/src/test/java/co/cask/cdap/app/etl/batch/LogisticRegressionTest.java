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

import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SparkSink} plugin type, using LogisticRegressionTrainer.
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

  @Test
  public void testSparkPlugins() throws Exception {
    // use the SparkSink to train a logistic regression model
    testSparkSink();
  }

  private void testSparkSink() throws Exception {
    /*
     * stream --> transform --> sparksink
     */
    String script = "function transform(input, emitter, context) {" +
      "  parts = input.body.split(',');" +
      "  text = parts[0].split(' ');" +
      "  input.isSpam = text[0] === 'spam' ? 1.0 : 0;" +
      "  text.shift();" +
      "  input.text = text.join(' ');" +
      "  input.boolField = parts[1];" +
      "  emitter.emit(input);" +
      "}";

    Map<String, String> sourceProp = ImmutableMap.of("name", "logisticRegressionTrainingStream",
                                                     "duration", "1d",
                                                     "format", "csv",
                                                     "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    Map<String, String> transformProp = ImmutableMap.of("script", script,
                                                        "schema", LogisticRegressionSpamMessage.SCHEMA.toString());

    String fieldsToClassify = LogisticRegressionSpamMessage.TEXT_FIELD + "," + LogisticRegressionSpamMessage.READ_FIELD;
    Map<String, String> sinkProp = ImmutableMap.of("fileSetName", "modelFileSet",
                                                   "path", "output",
                                                   "fieldsToClassify", fieldsToClassify,
                                                   "predictionField",
                                                   LogisticRegressionSpamMessage.SPAM_PREDICTION_FIELD,
                                                   "numClasses", "2");
    ETLPlugin sparkPlugin = new ETLPlugin("LogisticRegressionTrainer", SparkSink.PLUGIN_TYPE, sinkProp, null);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, sourceProp, null)))
      .addStage(new ETLStage("eventParser", new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE, transformProp, null)))
      .addStage(new ETLStage("customSink", sparkPlugin))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "customSink")
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    Id.Application appId = Id.Application.from(TEST_NAMESPACE, "LogicalRegressionSpamTrainer");
    ApplicationManager appManager = deployApplication(appId, request);

    // set up five spam messages and five non-spam messages to be used for classification
    List<String> trainingMessages = ImmutableList.of("spam buy our clothes,yes",
                                                     "spam sell your used books to us,yes",
                                                     "spam earn money for free,yes",
                                                     "spam this is definitely not spam,yes",
                                                     "spam you won the lottery,yes",
                                                     "ham how was your day,no",
                                                     "ham what are you up to,no",
                                                     "ham this is a genuine message,no",
                                                     "ham this is an even more genuine message,no",
                                                     "ham could you send me the report,no");

    Id.Stream stream = Id.Stream.from(Id.Namespace.DEFAULT, "logisticRegressionTrainingStream");

    // write records to source
    StreamManager streamManager = getTestManager().getStreamManager(stream);
    for (String spamMessage : trainingMessages) {
      streamManager.send(spamMessage);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }
}
