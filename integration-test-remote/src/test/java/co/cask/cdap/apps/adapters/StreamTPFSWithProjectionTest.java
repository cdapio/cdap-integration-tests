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
import co.cask.cdap.apps.adapters.dataset.TPFSService;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.template.etl.batch.source.StreamBatchSource;
import co.cask.cdap.template.etl.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

// TODO CDAP-3364: Ignoring this test case as StandaloneTester is unable to setup plugin classes properly right now.
// once that is fixed, we can remove this ignore.
/**
 * Integration test which tests the following:
 * <ul>
 * <li>{@link StreamBatchSource}</li>
 * <li>{@link ProjectionTransform} : Drop</li>
 * <li>{@link TimePartitionedFileSetDatasetAvroSink}</li>
 * <li>{@link TimePartitionedFileSetDatasetAvroSource}</li>
 */
@Ignore
public class StreamTPFSWithProjectionTest extends AdaptersTestBase {

  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");
  private static final String SOURCE_STREAM = "sourceStream";


  @Test
  public void testAdapters() throws Exception {

    //1. create a source stream and send an event
    Id.Stream sourceStreamId = createSourceStream(SOURCE_STREAM);
    streamClient.sendEvent(sourceStreamId, DUMMY_STREAM_EVENT);

    //2. Deploy an application with a service to get TPFS data for verification
    ApplicationManager applicationManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(TPFSService.class.getSimpleName());
    serviceManager.start();

    // 3. Run Stream To TPFS with Projection Transform Adapter
    final Id.Adapter streamToTPFSAdapterId = Id.Adapter.from(TEST_NAMESPACE, "StreamToTPFSWithProjection");
    ETLBatchConfig etlBatchConfig = constructStreamToTPFSConfig();
    runAndWait(TEMPLATE_ID, streamToTPFSAdapterId, etlBatchConfig);

    // 4. Run TPFS to TPFS Adapter where the source is the sink from the above adapter
    final Id.Adapter tpfsToTPFSAdapterId = Id.Adapter.from(TEST_NAMESPACE, "TPFSToTPFSWithProjection");
    etlBatchConfig = constructTPFSToTPFSConfig();
    runAndWait(TEMPLATE_ID, tpfsToTPFSAdapterId, etlBatchConfig);

    // both the adapters needs to run first so that the TPFS gets created and the service can access it.

    // 5. Verify data in TPFS
    verifyTPFSData(serviceManager, TPFSService.TPFS_1);
    verifyTPFSData(serviceManager, TPFSService.TPFS_2);

    serviceManager.stop();
    adapterClient.delete(streamToTPFSAdapterId);
    adapterClient.delete(tpfsToTPFSAdapterId);
  }

  private void verifyTPFSData(ServiceManager serviceManager, String tpfsName) throws IOException,
    UnauthorizedException {

    URL tpfsURL = new URL(serviceManager.getServiceURL(), TPFSService.TPFS_PATH +
      String.format("/%s?time=1", tpfsName));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, tpfsURL, getClientConfig().getAccessToken());
    List<IntegrationTestRecord> responseObject = ObjectResponse.<List<IntegrationTestRecord>>fromJsonBody(
      response, new TypeToken<List<IntegrationTestRecord>>() {
      }.getType()).getResponseObject();
    Assert.assertEquals("AAPL", responseObject.get(0).getTicker());
  }

  private ETLBatchConfig constructStreamToTPFSConfig() {
    ETLStage source = etlStageProvider.getStreamBatchSource(SOURCE_STREAM, "10m", "0d",
                                                            Formats.CSV, DUMMY_STREAM_EVENT_SCHEMA, "|");
    ETLStage sink = etlStageProvider.getTPFS(TPFSService.EVENT_SCHEMA, TPFSService.TPFS_1, null, null, null);
    ETLStage transform = new ETLStage("Projection", ImmutableMap.of("drop", "headers"));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  private ETLBatchConfig constructTPFSToTPFSConfig() {
    ETLStage source = etlStageProvider.getTPFS(TPFSService.EVENT_SCHEMA, TPFSService.TPFS_1, null, "10m", "0d");
    ETLStage sink = etlStageProvider.getTPFS(TPFSService.EVENT_SCHEMA, TPFSService.TPFS_2, null, null, null);
    ETLStage transform = etlStageProvider.getEmptyProjectionTranform();
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  private class IntegrationTestRecord {
    private final long ts;
    private final String ticker;
    private final double price;
    private final int num;

    IntegrationTestRecord(long ts, String ticker, double price, int num) {
      this.ticker = ticker;
      this.ts = ts;
      this.price = price;
      this.num = num;
    }

    public String getTicker() {
      return ticker;
    }
  }
}
