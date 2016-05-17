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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SparkSink} and {@link SparkCompute} plugin types, using
 * NaiveBayesTrainer and a NaiveBayesClassifier.
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
public class SparkPluginsTest extends ETLTestBase {

  @Test
  public void testSparkPlugins() throws Exception {
    // use the SparkSink to train a model
    testSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSparkCompute();
  }

  private void testSparkSink() throws Exception {
    /*
     * stream --> transform --> sparksink
     */
    String script = "function transform(input, emitter, context) {" +
      "  parts = input.body.split(' ');" +
      "  input.isSpam = parts[0] === 'spam' ? 1.0 : 0;" +
      "  parts.shift();" +
      "  input.text = parts.join(' ');" +
      "  emitter.emit(input);" +
      "}";

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE,
                                                     ImmutableMap.of("name", "trainingStream",
                                                                     "duration", "1d",
                                                                     "format", "text"),
                                                     null)))
      .addStage(new ETLStage("eventParser", new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE,
                                                           ImmutableMap.of("script", script,
                                                                           "schema", SpamMessage.SCHEMA.toString()),
                                                           null)))
      .addStage(new ETLStage("customSink",
                             new ETLPlugin("NaiveBayesTrainer", SparkSink.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "predictionField", SpamMessage.SPAM_PREDICTION_FIELD),
                                           null)))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "customSink")
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    ApplicationManager appManager = deployApplication(Id.Application.from(Id.Namespace.DEFAULT, "SpamTrainer"),
                                                      getBatchAppRequestV2(etlConfig));


    // set up five spam messages and five non-spam messages to be used for classification
    List<String> trainingMessages =
      ImmutableList.of("spam buy our clothes",
                       "spam sell your used books to us",
                       "spam earn money for free",
                       "spam this is definitely not spam",
                       "spam you won the lottery",
                       "ham how was your day",
                       "ham what are you up to",
                       "ham this is a genuine message",
                       "ham this is an even more genuine message",
                       "ham could you send me the report");
    // write records to source
    StreamManager streamManager =
      getTestManager().getStreamManager(Id.Stream.from(Id.Namespace.DEFAULT, "trainingStream"));
    for (String spamMessage : trainingMessages) {
      streamManager.send(spamMessage);
    }

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);
  }

  private void testSparkCompute() throws Exception {
    String textsToClassify = "textsToClassify";

    String script = "function transform(input, emitter, context) {" +
      "  input.text = input.body;" +
      "  emitter.emit(input);" +
      "}";

    Schema simpleMessageSchema =
      Schema.recordOf("simpleMessage", Schema.Field.of(SpamMessage.TEXT_FIELD, Schema.of(Schema.Type.STRING)));

    /**
     * stream --> transform --> sparkcompute --> tpfssink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE,
                                                     ImmutableMap.of("name", textsToClassify,
                                                                     "duration", "1d",
                                                                     "format", "text"),
                                                     null)))
      .addStage(new ETLStage("eventParser", new ETLPlugin("JavaScript", Transform.PLUGIN_TYPE,
                                                          ImmutableMap.of("script", script,
                                                                          "schema", simpleMessageSchema.toString()),
                                                          null)))
      .addStage(new ETLStage("sparkCompute",
                             new ETLPlugin("NaiveBayesClassifier", SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "predictionField", SpamMessage.SPAM_PREDICTION_FIELD),
                                           null)))
      .addStage(new ETLStage("sink", new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE,
                                                   ImmutableMap.of("name", "classifiedTexts",
                                                                   "schema", SpamMessage.SCHEMA.toString()),
                                                   null)))
      .addConnection("source", "eventParser")
      .addConnection("eventParser", "sparkCompute")
      .addConnection("sparkCompute", "sink")
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();


    ApplicationManager appManager = deployApplication(Id.Application.from(Id.Namespace.DEFAULT, "SpamClassifier"),
                                                      getBatchAppRequestV2(etlConfig));


    // write some some messages to be classified
    StreamManager streamManager =
      getTestManager().getStreamManager(Id.Stream.from(Id.Namespace.DEFAULT, textsToClassify));
    streamManager.send("how are you doing today");
    streamManager.send("free money money");
    streamManager.send("what are you doing today");
    streamManager.send("genuine report");

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);


    Set<SpamMessage> expected = new HashSet<>();
    expected.add(new SpamMessage("how are you doing today", 0.0));
    // only 'free money money' should be predicated as spam
    expected.add(new SpamMessage("free money money", 1.0));
    expected.add(new SpamMessage("what are you doing today", 0.0));
    expected.add(new SpamMessage("genuine report", 0.0));

    Assert.assertEquals(expected, getClassifiedMessages());
  }

  private Set<SpamMessage> getClassifiedMessages() throws ExecutionException, InterruptedException {
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult exploreExecutionResult =
      queryClient.execute(TEST_NAMESPACE, "SELECT * FROM dataset_classifiedTexts").get();

    Set<SpamMessage> classifiedMessages = new HashSet<>();
    while (exploreExecutionResult.hasNext()) {
      List<Object> columns = exploreExecutionResult.next().getColumns();
      classifiedMessages.add(new SpamMessage((String) columns.get(1), (double) columns.get(0)));
    }
    return classifiedMessages;
  }

}
