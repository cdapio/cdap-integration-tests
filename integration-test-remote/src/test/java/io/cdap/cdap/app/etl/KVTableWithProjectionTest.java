/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.app.etl;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.source.KVTableSource;
import io.cdap.plugin.common.Properties;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link KVTableSource} and ProjectionTransform convert function
 */
public class KVTableWithProjectionTest extends ETLTestBase {

  private static final String KVTABLE_NAME = "kvtable1";

  @Test
  public void testKVTableWithProjection() throws Exception {
    // 1. Ingest data
    ingestData();

    // 2. Run Table To KVTable with Projection Transform plugin
    ApplicationId appId = TEST_NAMESPACE.app("TableToKVTableWithProjection");

    ETLStage source = new ETLStage("TableSource",
                                   new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, SOURCE_DATASET,
                                                                 Properties.Table.PROPERTY_SCHEMA,
                                                                 DATASET_SCHEMA.toString()), null));
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
    ETLBatchConfig etlBatchConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlBatchConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    byte[] result = getKVTableDataset(KVTABLE_NAME).get().read("AAPL");
    Assert.assertNotNull(result);
    Assert.assertEquals(500.32, Bytes.toDouble(result), 0.001);
  }
}
