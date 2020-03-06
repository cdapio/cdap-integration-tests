package io.cdap.cdap.app.etl.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Bucket;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import io.cdap.cdap.app.etl.hub.commons.MarketPlugins;
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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

public class DLPTest extends DataprocETLTestBase {

  @Override
  protected void innerSetup() throws Exception {
    MarketPlugins.loadPlugin(artifactClient, "plugin-dlp-redact-filter", "dlp");
  }

  @Override
  protected void innerTearDown() throws Exception {

  }

  @Test
  public void testLoading() throws Exception {
    System.out.println("Loaded.");
  }

  @Test
  public void testRedaction() throws Exception {
    String prefix = "cdap-dlp-redact-test";
    String bucketName = String.format("%s-%s", prefix, UUID.randomUUID());
    Bucket bucket = createBucket(bucketName);

    bucket.create("test-input-ashau.csv", (
        "0,alice,alice@example.com\n"
      + "1,bob,bob@example.com\n"
      + "2,craig,craig@example.com").getBytes(StandardCharsets.UTF_8));

    Schema sourceSchema = Schema.recordOf("etlSchemaBody",
      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    ETLStage gcsSourceStage =
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

    Schema wranglerSchema = Schema.recordOf("etlSchemaBody",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("number", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)));
    Joiner directives = Joiner.on("\n");
    String wranglerDirectives = directives.join(
      "parse-as-csv :body ',' false",
      "rename body_1 number",
      "rename body_2 name",
      "rename body_3 email",
      "drop body");

    ETLStage wranglerTransformStage =
      new ETLStage("Wrangler",
        new ETLPlugin("Wrangler", Transform.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
          .put("field", "body")
          .put("precondition", "false")
          .put("threshold", "1")
          .put("schema", wranglerSchema.toString())
          .put("directives", wranglerDirectives).build(), null));

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
        // .put("suffix", "yyyy-MM-dd-HH-mm")
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-redaction")
        .put("path", createPath(bucket, "test-output-ashau.csv")).build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(gcsSourceStage)
      .addStage(wranglerTransformStage)
      .addStage(redactStage)
      .addStage(sinkStage)
      .addConnection(gcsSourceStage.getName(), wranglerTransformStage.getName())
      .addConnection(wranglerTransformStage.getName(), redactStage.getName())
      .addConnection(redactStage.getName(), sinkStage.getName())
      // .setDriverResources(new Resources(1024))
      // .setResources(new Resources(1024))
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("DLPRedactTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertGCSContents(bucket, "test-output-ashau.csv/part-m-00000",
      "alice,0,#################\n"
        + "bob,1,###############\n"
        + "craig,2,#################\n");
  }
}
