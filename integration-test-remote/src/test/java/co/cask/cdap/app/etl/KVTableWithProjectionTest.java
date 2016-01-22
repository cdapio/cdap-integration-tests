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
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests {@link KVTableSource} and {@link Properties.ProjectionTransform} convert function
 */
// https://issues.cask.co/browse/CDAP-4629
@Ignore
public class KVTableWithProjectionTest extends ETLTestBase {

  private static final String SOURCE_STREAM = "sourceStream";
  private static final String KVTABLE_NAME = "kvtable1";

  @Test
  public void testKVTableWithProjection() throws Exception {
    //1. create a source stream and send an event
    Id.Stream sourceStreamId = createSourceStream(SOURCE_STREAM);
    streamClient.sendEvent(sourceStreamId, DUMMY_STREAM_EVENT);

    // 2. Run Stream To KVTable with Projection Transform plugin
    Id.Application appId = Id.Application.from(TEST_NAMESPACE, "StreamToKVTableWithProjection");

    ETLStage source = etlStageProvider.getStreamBatchSource(SOURCE_STREAM, "10m", "0d", Formats.CSV,
                                                            DUMMY_STREAM_EVENT_SCHEMA, "|");
    ETLStage sink = etlStageProvider.getTableSource(KVTABLE_NAME, "ticker", "price");
    ETLStage transform = new ETLStage("ProjectionTransform1",
                                      new Plugin("Projection", ImmutableMap.of("convert", "ticker:bytes,price:bytes")));
    ETLBatchConfig etlBatchConfig = new ETLBatchConfig("*/10 * * * *", source, sink, Lists.newArrayList(transform));

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequest(etlBatchConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager("ETLMapReduce");
    mrManager.start();
    mrManager.waitForFinish(10, TimeUnit.MINUTES);

    byte[] result = getKVTableDataset(KVTABLE_NAME).get().read("AAPL");
    Assert.assertNotNull(result);
    Assert.assertEquals(500.32, Bytes.toDouble(result), 0.001);
  }
}
