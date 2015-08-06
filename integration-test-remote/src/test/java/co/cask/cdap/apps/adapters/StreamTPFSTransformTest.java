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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.apps.conversion.ConversionTestExample;
import co.cask.cdap.apps.conversion.ConversionTestService;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.ApplicationTemplateNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
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
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// TODO : Ignoring this test case as StandaloneTester is unable to setup plugin classes properly right now.
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
public class StreamTPFSTransformTest extends AudiTestBase {

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");
  private static final String SOURCE_STREAM = "sourceStream";
  public static final String TPFS_1 = "tpfs1";
  public static final String TPFS_2 = "tpfs2";
  private static final Gson GSON = new Gson();

  @Test
  public void testAdapters() throws Exception {

    //1. create a source stream and send an event
    StreamClient streamClient = getStreamClient();
    ETLStageProvider etlStageProvider = getEtlStageProvider();
    Id.Stream sourceStreamId = Id.Stream.from(TEST_NAMESPACE, SOURCE_STREAM);
    streamClient.create(sourceStreamId);
    streamClient.sendEvent(sourceStreamId, "AAPL|10|500.32");

    //2. Deploy an application with a service to get TPFS data for verification
    ApplicationManager applicationManager = deployApplication(ConversionTestExample.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(ConversionTestService.class.getSimpleName());
    serviceManager.start();

    final AdapterClient adapterClient = new AdapterClient(getClientConfig(), getRestClient());

    // 3. Run Stream To TPFS with Projection Transform Adapter
    final Id.Adapter streamToTPFSAdapterId = Id.Adapter.from(TEST_NAMESPACE, "StreamToTPFSWithProjection");
    ETLBatchConfig etlBatchConfig = constructStreamToTPFSConfig(etlStageProvider, SOURCE_STREAM, TPFS_1);
    runAdapter(adapterClient, streamToTPFSAdapterId, etlBatchConfig);

    // 4. Run TPFS to TPFS Adapter where the source is the sink from the above adapter
    final Id.Adapter tpfsToTPFSAdapterId = Id.Adapter.from(TEST_NAMESPACE, "TPFSToTPFSWithProjection");
    etlBatchConfig = constructTPFSToTPFSConfig(etlStageProvider, TPFS_1, TPFS_2);
    runAdapter(adapterClient, tpfsToTPFSAdapterId, etlBatchConfig);

    // both the adapters needs to run first so that the TPFS gets created and the service can access it.

    // 5. Verify data in TPFS
    verifyTPFSData(TPFS_1);
    verifyTPFSData(TPFS_2);

    serviceManager.stop();
    adapterClient.delete(streamToTPFSAdapterId);
    adapterClient.delete(tpfsToTPFSAdapterId);
  }

  private void runAdapter(AdapterClient adapterClient, Id.Adapter adapterId, ETLBatchConfig etlBatchConfig)
    throws BadRequestException, ApplicationTemplateNotFoundException, UnauthorizedException, IOException,
    AdapterNotFoundException, InterruptedException, ExecutionException, TimeoutException {

    AdapterConfig adapterConfig = new AdapterConfig("description", TEMPLATE_ID.getId(),
                                                    GSON.toJsonTree(etlBatchConfig));
    adapterClient.create(adapterId, adapterConfig);
    adapterClient.start(adapterId);
    waitForAdapterCompletion(adapterClient, adapterId);
    adapterClient.stop(adapterId);
  }

  private void waitForAdapterCompletion(final AdapterClient adapterClient, final Id.Adapter adapterId)
    throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        List<RunRecord> completedRuns =
          adapterClient.getRuns(adapterId, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, null);
        return !completedRuns.isEmpty();
      }
    }, 10, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);
  }

  private void verifyTPFSData(String tpfsName) throws IOException, UnauthorizedException {
    URL tpfsURL = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE,
                                                           String.format("apps/%s/services/%s/methods/%s?time=1",
                                                                         ConversionTestExample.class.getSimpleName(),
                                                                         ConversionTestService.class.getSimpleName(),
                                                                         tpfsName));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, tpfsURL, getClientConfig().getAccessToken());
    List<IntegrationTestRecord> responseObject = ObjectResponse.<List<IntegrationTestRecord>>fromJsonBody(
      response, new TypeToken<List<IntegrationTestRecord>>() {
      }.getType()).getResponseObject();
    Assert.assertEquals("AAPL", responseObject.get(0).getTicker());
  }

  private ETLBatchConfig constructStreamToTPFSConfig(ETLStageProvider etlStageProvider, String sourceName, String sinkName) {
    ETLStage source = etlStageProvider.getStreamBatchSource(sourceName, "10m", "0d", Formats.CSV, BODY_SCHEMA, "|");
    ETLStage sink = etlStageProvider.getTPFS(ConversionTestService.EVENT_SCHEMA, sinkName, null, null, null);
    ETLStage transform = new ETLStage("Projection", ImmutableMap.of("drop", "headers"));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  private ETLBatchConfig constructTPFSToTPFSConfig(ETLStageProvider etlStageProvider, String filesetName, String newFilesetName) {
    ETLStage source = etlStageProvider.getTPFS(ConversionTestService.EVENT_SCHEMA, filesetName, null, "10m", "0d");
    ETLStage sink = etlStageProvider.getTPFS(ConversionTestService.EVENT_SCHEMA, newFilesetName, null, null, null);
    ETLStage transform = etlStageProvider.getEmptyProjectionTranform();
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  private class IntegrationTestRecord {
    private long ts;
    private String ticker;
    private double price;
    private int num;

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
