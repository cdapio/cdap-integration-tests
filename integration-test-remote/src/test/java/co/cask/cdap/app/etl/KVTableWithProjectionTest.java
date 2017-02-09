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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests {@link KVTableSource} and {@link Properties.ProjectionTransform} convert function
 */
public class KVTableWithProjectionTest extends ETLTestBase {

  private static final String SOURCE_STREAM = "sourceStream";
  private static final String KVTABLE_NAME = "kvtable1";

  @Test
  public void testKVTableWithProjection() throws Exception {
    //1. create a source stream and send an event
    StreamId sourceStreamId = createSourceStream(SOURCE_STREAM);
    streamClient.sendEvent(sourceStreamId, DUMMY_STREAM_EVENT);

    // 2. Run Stream To KVTable with Projection Transform plugin
    ApplicationId appId = TEST_NAMESPACE.app("StreamToKVTableWithProjection");

    ETLStage source = etlStageProvider.getStreamBatchSource(SOURCE_STREAM, "10m", "0d", Formats.CSV,
                                                            DUMMY_STREAM_EVENT_SCHEMA, "|");
    ETLStage sink = new ETLStage("sink",
                                 new ETLPlugin("KVTable", BatchSink.PLUGIN_TYPE,
                                               ImmutableMap.of("name", KVTABLE_NAME,
                                                               "key.field", "ticker",
                                                               "value.field", "price"),
                                               null));
    ETLStage transform = new ETLStage("ProjectionTransform1",
                                      new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                    ImmutableMap.of("convert", "ticker:bytes,price:bytes"),
                                                    null));
    ETLBatchConfig etlBatchConfig = ETLBatchConfig.builder("*/10 * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlBatchConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    byte[] result = getKVTableDataset(KVTABLE_NAME).get().read("AAPL");
    Assert.assertNotNull(result);
    Assert.assertEquals(500.32, Bytes.toDouble(result), 0.001);
  }
}
