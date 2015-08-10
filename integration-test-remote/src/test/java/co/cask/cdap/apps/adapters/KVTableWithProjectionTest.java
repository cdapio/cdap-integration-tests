/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps.adapters;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.apps.adapters.dataset.DatasetAccessApp;
import co.cask.cdap.apps.adapters.dataset.KVTableService;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.batch.source.KVTableSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

// TODO : Ignoring this test case as StandaloneTester is unable to setup plugin classes properly right now.
// once that is fixed, we can remove this ignore.

/**
 * Tests {@link KVTableSource} and {@link Properties.ProjectionTransform} convert function
 */
@Ignore
public class KVTableWithProjectionTest extends AdaptersTestBase {

  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");
  private static final String SOURCE_STREAM = "SourceStream";

  @Test
  public void testAdapters() throws Exception {

    //1. create a source stream and send an event
    Id.Stream sourceStreamId = createSourceStream(SOURCE_STREAM);
    streamClient.sendEvent(sourceStreamId, DUMMY_STREAM_EVENT);

    //2. Deploy an application with a service to access key value table for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(KVTableService.class.getSimpleName());
    serviceManager.start();

    // 3. Run Stream To KVTable with Projection Transform Adapter
    final Id.Adapter streamToKVTableAdapterId = Id.Adapter.from(TEST_NAMESPACE, "StreamToKVTableWithProjection");
    ETLBatchConfig etlBatchConfig = constructStreamToTableConfig();
    runAndWait(TEMPLATE_ID, streamToKVTableAdapterId, etlBatchConfig);

    verifyKVTableData();

    serviceManager.stop();
    adapterClient.delete(streamToKVTableAdapterId);
  }

  private ETLBatchConfig constructStreamToTableConfig() {
    ETLStage source = etlStageProvider.getStreamBatchSource(SOURCE_STREAM, "10m", "0d", Formats.CSV,
                                                            DUMMY_STREAM_EVENT_SCHEMA, "|");
    ETLStage sink = etlStageProvider.getTableSource(KVTableService.KV_TABLE_NAME, "ticker", "price");
    ETLStage transform = new ETLStage("Projection", ImmutableMap.of("convert", "ticker:bytes,price:bytes"));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  private void verifyKVTableData() throws IOException, UnauthorizedException {
    URL tpfsURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                           String.format("apps/%s/services/%s/methods/%s/%s?%s=%s",
                                                                         DatasetAccessApp.class.getSimpleName(),
                                                                         KVTableService.class.getSimpleName(),
                                                                         KVTableService.KVTABLE_PATH,
                                                                         KVTableService.KV_TABLE_NAME,
                                                                         KVTableService.KEY, "AAPL"));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, tpfsURL, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(500.32, Bytes.toDouble(Bytes.toBytes(response.getResponseBodyAsString())), 0.001);
  }
}
