/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.etl.wrangler;


import co.cask.cdap.api.Resources;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.app.etl.dataset.DatasetAccessApp;
import co.cask.cdap.app.etl.dataset.SnapshotFilesetService;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrangler test to test Wrangler Transform functionalities
 */
public class WranglerTest extends ETLTestBase {

  @Test
  public void testWranglerSpark() throws Exception {
    testWrangler(Engine.SPARK);
  }

  @Test
  public void testWranglerMR() throws Exception {
    testWrangler(Engine.MAPREDUCE);
  }

  private void testWrangler(Engine engine) throws Exception {
    //create a source stream and then send an event
    String streamName = "personal_info";
    StreamId sourceStreamId = createSourceStream(streamName);
    streamClient.sendEvent(sourceStreamId, "1,David,123 Everett St,94303,123-456-7890");
    streamClient.sendEvent(sourceStreamId, "2,Albert,456 Everett St,94304,123-456-7880");
    streamClient.sendEvent(sourceStreamId, "3,Daniel,789 Everett St,94305,123-456-7870");
    streamClient.sendEvent(sourceStreamId, "4,Alex,000 Everett St,94306,123-456-7860");
    streamClient.sendEvent(sourceStreamId, "5,Daniel,789 Everett St,94303,123-456-7870");

    Schema streamSchema = Schema.recordOf("etlSchemaBody",
                                          Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

    ETLStage streamSourceStage =
      new ETLStage("Stream",
                   new ETLPlugin("Stream",
                                 BatchSource.PLUGIN_TYPE,
                                 ImmutableMap.of("schema", streamSchema.toString(),
                                                 "format", "text",
                                                 "name", streamName,
                                                 "duration", "1d"), null));

    Schema wranglerSchema = Schema.recordOf("etlSchemaBody",
                                            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("street", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("zipcode", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("number", Schema.of(Schema.Type.STRING)));

    //properties for the wranglerTransformStage
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder();
    Joiner directives = Joiner.on("\n");
    String wranglerDirectives = directives.join("drop ts", "drop headers", "parse-as-csv body ,", "drop body",
                                                "rename body_1 id", "rename body_2 name", "rename body_3 street",
                                                "rename body_4 zipcode", "rename body_5 number");
    builder.put("field", "*")
      .put("precondition", "false")
      .put("threshold", "1")
      .put("schema", wranglerSchema.toString())
      .put("directives", wranglerDirectives);

    ETLStage wranglerTransformStage =
      new ETLStage("Wrangler",
                   new ETLPlugin("Wrangler", Transform.PLUGIN_TYPE, builder.build(), null));

    ETLStage groupStage =
      new ETLStage("GroupByAggregate",
                   new ETLPlugin("GroupByAggregate",
                                 BatchAggregator.PLUGIN_TYPE,
                                 ImmutableMap.of("groupByFields", "zipcode",
                                                 "aggregates", "people:Count(id)"), null));

    Schema sinkSchema = Schema.recordOf("etlSchemaBody",
                                        Schema.Field.of("zipcode", Schema.of(Schema.Type.STRING)),
                                        Schema.Field.of("people", Schema.of(Schema.Type.LONG)));

    ETLStage sinkStage =
      new ETLStage("SnapshotAvro", new ETLPlugin("SnapshotAvro", BatchSink.PLUGIN_TYPE,
                                                 ImmutableMap.of("compressionCodec", "None",
                                                                 "schema", sinkSchema.toString(),
                                                                 "name", "SnapshotAvro"), null));

    ETLBatchConfig config = ETLBatchConfig.builder("0 * * * *")
      .addStage(streamSourceStage)
      .addStage(wranglerTransformStage)
      .addStage(groupStage)
      .addStage(sinkStage)
      .addConnection(streamSourceStage.getName(), wranglerTransformStage.getName())
      .addConnection(wranglerTransformStage.getName(), groupStage.getName())
      .addConnection(groupStage.getName(), sinkStage.getName())
      .setEngine(engine)
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("dataprep");
    List<ArtifactSummary> artifactSummaryList = artifactClient.list(TEST_NAMESPACE.getNamespaceId(),
                                                                    ArtifactScope.SYSTEM);
    AppRequest appRequest = getWranglerAppRequest(artifactSummaryList);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ServiceManager wranglerServiceManager = startService(appManager, "service");

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(config);
    appId = TEST_NAMESPACE.app("wrangler-test");
    ApplicationManager testAppManager = deployApplication(appId, request);

    // run the pipeline
    WorkflowManager workflowManager = testAppManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    // Deploy an application with a service to get partitionedFileset data for verification
    appManager = deployApplication(DatasetAccessApp.class);
    ServiceManager serviceManager = startService(appManager, SnapshotFilesetService.class.getSimpleName());

    org.apache.avro.Schema avroOutputSchema = new org.apache.avro.Schema.Parser().parse(sinkSchema.toString());
    GenericRecord record1 = new GenericRecordBuilder(avroOutputSchema)
      .set("zipcode", "94303")
      .set("people", (long) 2)
      .build();

    GenericRecord record2 = new GenericRecordBuilder(avroOutputSchema)
      .set("zipcode", "94304")
      .set("people", (long) 1)
      .build();

    GenericRecord record3 = new GenericRecordBuilder(avroOutputSchema)
      .set("zipcode", "94305")
      .set("people", (long) 1)
      .build();

    GenericRecord record4 = new GenericRecordBuilder(avroOutputSchema)
      .set("zipcode", "94306")
      .set("people", (long) 1)
      .build();

    Set<GenericRecord> expected = ImmutableSet.of(record1, record2, record3, record4);
    Set<GenericRecord> actual = readOutput(serviceManager, "SnapshotAvro", sinkSchema);
    // verfiy output
    Assert.assertEquals(expected, actual);

  }

  private ServiceManager startService(ApplicationManager appManager, String service) throws InterruptedException,
    ExecutionException, TimeoutException {
    ServiceManager serviceManager = appManager.getServiceManager(service);
    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    return serviceManager;
  }

  private Set<GenericRecord> readOutput(ServiceManager serviceManager, String sink, Schema schema)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL pfsURL = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                         String.format("read/%s", sink));
    HttpResponse response = getRestClient().execute(HttpMethod.GET, pfsURL, getClientConfig().getAccessToken());

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    Map<String, byte[]> map = ObjectResponse.<Map<String, byte[]>>fromJsonBody(
      response, new TypeToken<Map<String, byte[]>>() { }.getType()).getResponseObject();

    return parseOutput(map, schema);
  }


  private Set<GenericRecord> parseOutput(Map<String, byte[]> contents, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    Set<GenericRecord> records = new HashSet<>();
    for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      try (DataFileStream<GenericRecord> fileStream = new DataFileStream<>(
        new ByteArrayInputStream(entry.getValue()), datumReader)) {
        for (GenericRecord record : fileStream) {
          records.add(record);
        }
      }
    }
    return records;
  }
}
