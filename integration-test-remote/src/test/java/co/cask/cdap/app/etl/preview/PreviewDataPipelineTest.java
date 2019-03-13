/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package co.cask.cdap.app.etl.preview;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.Tasks;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Preview test for data pipeline
 */
public class PreviewDataPipelineTest extends ETLTestBase {
  private static final Gson GSON = new GsonBuilder()
                                     .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
                                     .create();
  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("etlschema",
                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("etlschema",
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("price", Schema.of(Schema.Type.FLOAT)));
  private static final Type MAP_STRING_STRUCTURED_LIST =
    new TypeToken<Map<String, List<StructuredRecord>>>() { }.getType();

  @Test
  public void testDataPipelinePreviewRun() throws Exception {
    String tableName = "PreviewTable";
    ETLStage source = new ETLStage("tableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, tableName,
      Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("CSVParser", Transform.PLUGIN_TYPE,
                                                    ImmutableMap.of("field", "body",
                                                                    "schema", OUTPUT_SCHEMA.toString()), null));

    ETLStage sink = new ETLStage("sink",
                                 new ETLPlugin("TPFSParquet", BatchSink.PLUGIN_TYPE,
                                               ImmutableMap.of("name", "previewTPFS",
                                                               "schema", OUTPUT_SCHEMA.toString()), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
                                 .addStage(source)
                                 .addStage(transform)
                                 .addStage(sink)
                                 .addConnection(source.getName(), transform.getName())
                                 .addConnection(transform.getName(), sink.getName())
                                 .setNumOfRecordsPreview(100)
                                 .build();

    // Construct the preview config with the program name and program type
    PreviewConfig previewConfig = new PreviewConfig(SmartWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.emptyMap(), 10);
    AppRequest<ETLBatchConfig> request =
      new AppRequest<>(new ArtifactSummary("cdap-data-pipeline", version, ArtifactScope.SYSTEM),
                       etlConfig, previewConfig);

    // put data in source
    DataSetManager<Table> inputManager = getTableDataset(tableName);
    putValue(inputManager.get(), 1,  "Banana, 1.0");
    putValue(inputManager.get(), 2,  "Apple, 2.0");
    putValue(inputManager.get(), 3,  "Watermelon, 3.0");

    ApplicationId previewId = startPreview(request);

    Tasks.waitFor(PreviewStatus.Status.COMPLETED, () -> getPreviewStatus(previewId).getStatus(),
                  2, TimeUnit.MINUTES, 500, TimeUnit.MILLISECONDS);

    // verify the data in source
    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(SOURCE_SCHEMA).set("body", "Banana, 1.0").build());
    expected.add(StructuredRecord.builder(SOURCE_SCHEMA).set("body", "Apple, 2.0").build());
    expected.add(StructuredRecord.builder(SOURCE_SCHEMA).set("body", "Watermelon, 3.0").build());
    verifyRecords(expected, previewId, source.getName());

    // verify the data in transform
    expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(OUTPUT_SCHEMA).set("name", "Banana").set("price", 1.0).build());
    expected.add(StructuredRecord.builder(OUTPUT_SCHEMA).set("name", "Apple").set("price", 2.0).build());
    expected.add(StructuredRecord.builder(OUTPUT_SCHEMA).set("name", "Watermelon").set("price", 3.0).build());
    verifyRecords(expected, previewId, transform.getName());

    // We do not verify sink records since the record will not be structured record and the output is not shown in UI
  }

  private ApplicationId startPreview(AppRequest<ETLBatchConfig> request) throws Exception {
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "previews");
    HttpResponse response = getRestClient().execute(HttpRequest.post(url).withBody(GSON.toJson(request)).build(),
                                                    getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), ApplicationId.class);
  }

  private PreviewStatus getPreviewStatus(ApplicationId previewId) throws Exception {
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, String.format("previews/%s/status",
                                                                                     previewId.getApplication()));
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(),
                                                    getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), PreviewStatus.class);
  }

  private Map<String, List<StructuredRecord>> getRecords(ApplicationId previewId, String tracerName) throws Exception {
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, String.format("previews/%s/tracers/%s",
                                                                                     previewId.getApplication(),
                                                                                     tracerName));
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(),
                                                    getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRUCTURED_LIST);
  }

  private void verifyRecords(List<StructuredRecord> expected, ApplicationId previewId,
                             String stageName) throws Exception {
    Map<String, List<StructuredRecord>> actualRecords = getRecords(previewId, stageName);
    // in preview we only record output data
    Assert.assertEquals(1, actualRecords.size());
    Assert.assertEquals(expected.size(), actualRecords.get("records.out").size());
    Assert.assertEquals(expected, actualRecords.get("records.out"));
  }

  private void putValue(Table table, int index, String body) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("body", body);
    table.put(put);
  }
}
