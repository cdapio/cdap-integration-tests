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
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Integration Test for NGramTransform.
 */
public class NGramTransformTest extends ETLTestBase {
    private static final Schema OUTPUT_SCHEMA = Schema.recordOf("outputSchema",
            Schema.Field.of("ngrams", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    @Test
    public void testNGramTransformWithTwoTerms() throws Exception {
        String text = "textToNGramTwo";
        ApplicationManager appManager = deployApplication(text, OUTPUT_SCHEMA, "/", "2");
        StreamManager streamManager =
                getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, text));
        streamManager.send("hi/hello/hie");
        streamManager.send("how/are/you");
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(10, TimeUnit.MINUTES);
        Assert.assertEquals(getExpectedForTwoTerms(), getNgramTerms());
    }

    @Test
    public void testNGramTransformWithThreeTerms() throws Exception {
        String text = "textToNGramThree";
        ApplicationManager appManager = deployApplication(text, OUTPUT_SCHEMA, ",", "3");
        StreamManager streamManager =
                getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, text));
        streamManager.send("hi,hello,hie,hi");
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(10, TimeUnit.MINUTES);
        Assert.assertEquals(getExpectedForThreeTerms(), getNgramTerms());
    }

    @Test(expected = NullPointerException.class)
    //ngramSize is mandatory and if its null then NullPointerException is expected.
    public void testNullNGramSize() throws Exception {
        deployApplication("NegativeTestNGramSize", OUTPUT_SCHEMA, ",", null);
    }

    private ApplicationManager deployApplication(String textsForNgram, Schema outputSchema,
                                                 String delimiter, String terms) throws Exception {
        Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
                .put("name", textsForNgram)
                .put("duration", "1d")
                .put("format", "text")
                .build();
        Map<String, String> tokenizerProperties = new ImmutableMap.Builder<String, String>()
                .put("outputColumn", "words")
                .put("columnToBeTokenized", "body")
                .put("pattern", delimiter)
                .build();
        Map<String, String> ngramProperties = new ImmutableMap.Builder<String, String>()
                .put("outputField", "ngrams")
                .put("fieldToBeTransformed", "words")
                .put("ngramSize", terms)
                .build();
        Map<String, String> sinkProperties = new ImmutableMap.Builder<String, String>()
                .put("name", "ngramTable")
                .put("schema", outputSchema.toString())
                .build();
        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(new ETLStage("source", new ETLPlugin("Stream",
                        BatchSource.PLUGIN_TYPE, sourceProperties, null)))
                .addStage(new ETLStage("tokenizerSpark", new ETLPlugin("Tokenizer",
                        SparkCompute.PLUGIN_TYPE, tokenizerProperties, null)))
                .addStage(new ETLStage("ngramSpark", new ETLPlugin("NGramTransform",
                        SparkCompute.PLUGIN_TYPE, ngramProperties, null)))
                .addStage(new ETLStage("sink", new ETLPlugin("TPFSAvro",
                        BatchSink.PLUGIN_TYPE, sinkProperties, null)))
                .addConnection("source", "tokenizerSpark")
                .addConnection("tokenizerSpark", "ngramSpark")
                .addConnection("ngramSpark", "sink")
                .build();
        return deployApplication(Id.Application.from(TEST_NAMESPACE, "NGramTransform"),
                getBatchAppRequestV2(etlConfig));
    }

    private Set<String> getExpectedForTwoTerms() {
        Set<String> expectedSet = new HashSet();
        expectedSet.add("[\"hi hello\",\"hello hie\"]");
        expectedSet.add("[\"how are\",\"are you\"]");
        return expectedSet;
    }

    private Set<String> getExpectedForThreeTerms() {
        Set<String> expectedSet = new HashSet();
        expectedSet.add("[\"hi hello hie\",\"hello hie hi\"]");
        return expectedSet;
    }

    private Set<String> getNgramTerms() throws ExecutionException, InterruptedException {
        QueryClient queryClient = new QueryClient(getClientConfig());
        ExploreExecutionResult exploreExecutionResult =
                queryClient.execute(TEST_NAMESPACE, "SELECT * FROM dataset_ngramTable").get();

        Set<String> classifiedMessages = new HashSet();
        while (exploreExecutionResult.hasNext()) {
            List<Object> columns = exploreExecutionResult.next().getColumns();
            classifiedMessages.add((String) columns.get(0));
        }
        return classifiedMessages;
    }
}
