/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Bucket;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import io.cdap.cdap.app.etl.hub.commons.MarketPlugins;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

/**
 * Test CDAP's plugin for GCP Data Loss Prevention (DLP), after loading it from the market.
 */
public class DLPTest extends DataprocETLTestBase {

  private ArtifactId pluginArtifactId;
  private Bucket bucket;
  private ETLStage gcsSourceStage, wranglerTransformStage;

  @Override
  protected void innerSetup() throws Exception {
    initializeGCS();
    pluginArtifactId = MarketPlugins.loadPlugin(artifactClient, "plugin-dlp-redact-filter", "1.0.2", "dlp",
      "default");
  }

  @Override
  protected void innerTearDown() throws Exception {
    artifactClient.delete(pluginArtifactId);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    String testCSVInput = (
        "0,alice,alice@example.com\n"
      + "1,bob,bob@example.com\n"
      + "2,craig,craig@example.com");

    String prefix = "cdap-dlp-test";
    String bucketName = String.format("%s-%s", prefix, UUID.randomUUID());
    bucket = createBucket(bucketName);
    bucket.create("test-input-ashau.csv", testCSVInput.getBytes(StandardCharsets.UTF_8));

    Schema sourceSchema = Schema.recordOf("etlSchemaBody",
      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    Schema wranglerSchema = Schema.recordOf("etlSchemaBody",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("number", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)));

    gcsSourceStage =
      new ETLStage("GCS",
        new ETLPlugin("GCSFile",
          BatchSource.PLUGIN_TYPE,
          new ImmutableMap.Builder<String, String>()
            .put("project", getProjectId())
            .put("format", "text")
            .put("serviceFilePath", "auto-detect")
            .put("schema", sourceSchema.toString())
            .put("referenceName", "load-user-data")
            .put("path", createPath(bucket, "test-input-ashau.csv")).build(), null));

    Joiner directives = Joiner.on("\n");
    String wranglerDirectives = directives.join(
      "parse-as-csv :body ',' false",
      "rename body_1 number",
      "rename body_2 name",
      "rename body_3 email",
      "drop body");

    wranglerTransformStage =
      new ETLStage("Wrangler",
        new ETLPlugin("Wrangler", Transform.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
          .put("field", "body")
          .put("precondition", "false")
          .put("threshold", "1")
          .put("schema", wranglerSchema.toString())
          .put("directives", wranglerDirectives).build(), null));
  }

  @Test
  public void testRedaction() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String fieldsToTransform = objectMapper.writeValueAsString(Collections.singletonList(
      objectMapper.writeValueAsString(new ImmutableMap.Builder<String, Object>()
        .put("fields", "email")
        .put("transform", "MASKING")
        .put("filters", "NONE")
        .put("transformProperties", new ImmutableMap.Builder<String, String>()
          .put("numberToMask", "0")
          .put("reverseOrder", "false")
          .put("charsToIgnoreEnum", "COMMON_CHARS_TO_IGNORE_UNSPECIFIED")
          .put("maskingChar", "#")
          .build())
        .build())));

    ETLStage redactStage =
      new ETLStage("Redact",
        new ETLPlugin("SensitiveRecordRedaction", Transform.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
          .put("customTemplateEnabled", "true")
          .put("serviceFilePath", "auto-detect")
          .put("project", getProjectId())
          .put("fieldsToTransform", fieldsToTransform)
          .put("templateId", "vravish-dlp-template").build(), null));

    ETLStage sinkStage =
      new ETLStage("GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
        .put("project", getProjectId())
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-redaction")
        .put("path", createPath(bucket, "test-output-ashau")).build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(gcsSourceStage)
      .addStage(wranglerTransformStage)
      .addStage(redactStage)
      .addStage(sinkStage)
      .addConnection(gcsSourceStage.getName(), wranglerTransformStage.getName())
      .addConnection(wranglerTransformStage.getName(), redactStage.getName())
      .addConnection(redactStage.getName(), sinkStage.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("DLPRedactTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertGCSContents(bucket, "test-output-ashau/part-m-00000",
      "alice,0,#################\n"
        + "bob,1,###############\n"
        + "craig,2,#################\n");
  }

  @Test
  public void testFilter() throws Exception {
    ETLStage filterStage =
      new ETLStage("PII Filter",
        new ETLPlugin("SensitiveRecordFilter", SplitterTransform.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
          .put("entire-record", "true")
          .put("on-error", "stop-on-error")
          .put("serviceFilePath", "auto-detect")
          .put("project", getProjectId())
          .put("template-id", "vravish-dlp-template").build(), null));

    ETLStage sinkStage1 =
      new ETLStage("GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
        .put("project", getProjectId())
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-filter")
        .put("path", createPath(bucket, "test-output-ashau-sensitive")).build(), null));

    ETLStage sinkStage2 =
      new ETLStage("copy of GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
        .put("project", getProjectId())
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-filter")
        .put("path", createPath(bucket, "test-output-ashau-nonsensitive")).build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(gcsSourceStage)
      .addStage(wranglerTransformStage)
      .addStage(filterStage)
      .addStage(sinkStage1)
      .addStage(sinkStage2)
      .addConnection(gcsSourceStage.getName(), wranglerTransformStage.getName())
      .addConnection(wranglerTransformStage.getName(), filterStage.getName())
      .addConnection(filterStage.getName(), sinkStage1.getName(), "Sensitive")
      .addConnection(filterStage.getName(), sinkStage2.getName(), "Non-Sensitive")
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("DLPFilterTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertGCSContents(bucket, "test-output-ashau-sensitive/part-m-00000",
      "alice,0,alice@example.com\n"
        + "bob,1,bob@example.com\n"
        + "craig,2,craig@example.com\n");
    assertNotExists(bucket, "test-output-ashau-nonsensitive/part-m-00000");
  }
}
