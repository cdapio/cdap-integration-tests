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

package co.cask.cdap.apps;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class IntegrationTest extends AudiTestBase {

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Schema EVENT_SCHEMA = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  protected final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");

  @Test
  public void testAdapter() throws Exception {
    StreamClient streamClient = new StreamClient(getClientConfig());
    streamClient.create("stream1");
    streamClient.sendEvent("stream1", "AAPL|10|500.32");

    AdapterClient adapterClient = new AdapterClient(getClientConfig());
    String filesetName = "temp";
    ETLBatchConfig etlBatchConfig = constructETLBatchConfig(filesetName, "TPFSAvro");
    final Gson GSON = new Gson();
    AdapterConfig adapterConfig = new AdapterConfig("description", TEMPLATE_ID.getId(),
                                                    GSON.toJsonTree(etlBatchConfig));
    adapterClient.create("test1", adapterConfig);
    adapterClient.start("test1");

    ApplicationManager applicationManager = deployApplication(ConversionTestExample.class);
    ServiceManager serviceManager = applicationManager.getServiceManager("ConversionTestService");
    serviceManager.start();

    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        RESTClient restClient = new RESTClient(getClientConfig());
        HttpResponse response = restClient.execute(HttpMethod.GET, getClientConfig().resolveNamespacedURLV3("adapters/test1/runs"), getClientConfig().getAccessToken());
        List<RunRecord> responseObject = ObjectResponse.<List<RunRecord>>fromJsonBody(
          response, new TypeToken<List<RunRecord>>() {
          }.getType()).getResponseObject();
        System.out.println(responseObject);
        for (RunRecord runRecord : responseObject) {
          if ((ProgramRunStatus.COMPLETED).equals(runRecord.getStatus())) {
            return true;
          }
        }
        return false;
      }
    }, 10, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

    adapterClient.stop("test1");
    adapterClient.delete("test1");

    RESTClient restClient = new RESTClient(getClientConfig());
    HttpResponse response = restClient.execute(HttpMethod.GET, getClientConfig().
      resolveNamespacedURLV3("apps/ConversionTestExample/services/ConversionTestService/methods/tpfs?time=1"),
                                               getClientConfig().getAccessToken());
    List<GenericData.Record> responseObject = ObjectResponse.<List<GenericData.Record>>fromJsonBody(
      response, new TypeToken<List<GenericData.Record>>() { }.getType()).getResponseObject();
    serviceManager.stop();
  }
  private ETLBatchConfig constructETLBatchConfig(String fileSetName, String sinkType) {
    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, "stream1")
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());
    ETLStage sink = new ETLStage(sinkType,
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                 EVENT_SCHEMA.toString(),
                                                 Properties.TimePartitionedFileSetDataset.TPFS_NAME, fileSetName));
    ETLStage transform = new ETLStage("Projection", ImmutableMap.<String, String>of("drop", "headers"));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }
}
