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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Integration Test for Tokenizer.
 */

public class TokenizerTest extends ETLTestBase {
    private static final Schema OUTPUT_SCHEMA = Schema.recordOf("outputSchema",
            Schema.Field.of("words", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    @Test
    public void testTokenizer() throws Exception {
        String textsToTokenize = "textToTokenize";
        ApplicationManager appManager = deployApplication(textsToTokenize, OUTPUT_SCHEMA, "/");
        StreamManager streamManager =
                getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, textsToTokenize));
        streamManager.send("cask data/application platform");
        streamManager.send("spark is/an engine");
        streamManager.send("cdap belongs/to apache");
        // manually trigger the pipeline
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(10, TimeUnit.MINUTES);
        Assert.assertEquals(getExpected(), getTokenizedMessages());
    }

    @Test
    public void testTokenizerCommaPattern() throws Exception {
        String textsToTokenize = "commaToTokenize";
        ApplicationManager appManager = deployApplication(textsToTokenize, OUTPUT_SCHEMA, ",");
        StreamManager streamManager =
                getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, textsToTokenize));
        streamManager.send("cask data,application platform");
        streamManager.send("spark is,an engine");
        streamManager.send("cdap belongs,to apache");
        // manually trigger the pipeline
        WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
        workflowManager.start();
        workflowManager.waitForFinish(10, TimeUnit.MINUTES);
        Assert.assertEquals(getExpected(), getTokenizedMessages());
    }

    @Test(expected = NullPointerException.class)
    public void testNullPattern() throws Exception {
        //pattern is mandatory and if its null then NullPointerException is expected.
        deployApplication("NegativeTestForPattern", OUTPUT_SCHEMA, null);
    }

    private ApplicationManager deployApplication(String textsToTokenize, Schema outputSchema, String pattern)
            throws Exception {
        ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
                .addStage(new ETLStage("source", new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE,
                        ImmutableMap.of("name", textsToTokenize,
                                "duration", "1d",
                                "format", "text"),
                        null)))
                .addStage(new ETLStage("sparkCompute",
                        new ETLPlugin("Tokenizer", SparkCompute.PLUGIN_TYPE,
                                ImmutableMap.of("outputColumn", "words",
                                        "columnToBeTokenized", "body",
                                        "pattern", pattern),
                                null)))
                .addStage(new ETLStage("sink", new ETLPlugin("TPFSAvro", BatchSink.PLUGIN_TYPE,
                        ImmutableMap.of("name", "tokenTable",
                                "schema", outputSchema.toString()),
                        null)))
                .addConnection("source", "sparkCompute")
                .addConnection("sparkCompute", "sink")
                .build();
        return deployApplication(Id.Application.from(TEST_NAMESPACE, "Tokenizer"),
                getBatchAppRequestV2(etlConfig));
    }

    private Set<String> getExpected() {
        Set<String> expectedSet = new HashSet();
        expectedSet.add("[\"cask data\",\"application platform\"]");
        expectedSet.add("[\"spark is\",\"an engine\"]");
        expectedSet.add("[\"cdap belongs\",\"to apache\"]");
        return expectedSet;
    }

    private Set<String> getTokenizedMessages() throws ExecutionException, InterruptedException {
        QueryClient queryClient = new QueryClient(getClientConfig());
        ExploreExecutionResult exploreExecutionResult =
                queryClient.execute(TEST_NAMESPACE, "SELECT * FROM dataset_tokenTable").get();

        Set<String> classifiedMessages = new HashSet();
        while (exploreExecutionResult.hasNext()) {
            List<Object> columns = exploreExecutionResult.next().getColumns();
            classifiedMessages.add((String) columns.get(0));
        }
        return classifiedMessages;
    }
}
