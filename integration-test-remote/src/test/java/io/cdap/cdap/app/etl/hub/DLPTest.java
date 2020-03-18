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
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.privacy.dlp.v2.CreateInspectTemplateRequest;
import com.google.privacy.dlp.v2.DeleteInspectTemplateRequest;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.ProjectName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import io.cdap.cdap.common.ArtifactNotFoundException;
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
import io.cdap.cdap.test.ApplicationManager;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test CDAP's plugin for GCP Data Loss Prevention (DLP), after loading it from the market.
 */
public class DLPTest extends DataprocETLTestBase {

  private static Storage storage;
  private static Bucket bucket;
  private static ETLStage gcsSourceStage;
  private static InspectTemplate inspectTemplate;
  private static DlpServiceClient dlpServiceClient;
  private static String templateId;
  private static final String INSPECT_TEMPLATE_NAME = "test-dlp-template";

  @Override
  protected void innerSetup() throws Exception {
    try {
      artifactClient.getPluginSummaries(TEST_NAMESPACE.artifact("dlp", "1.0.2"),
                                        Transform.PLUGIN_TYPE);
    } catch (ArtifactNotFoundException e) {
      installPluginFromHub("plugin-dlp-redact-filter", "dlp", "1.0.2");
    }
  }

  @Override
  protected void innerTearDown() throws Exception {
  }

  private static String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    storage = StorageOptions.newBuilder()
      .setProjectId(DataprocETLTestBase.getProjectId())
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(getServiceAccountCredentials()
                                   .getBytes(StandardCharsets.UTF_8))))
      .build().getService();

    String testCSVInput =
        "0,alice,alice@example.com\n"
      + "1,bob,bob@example.com\n"
      + "2,craig,craig@example.com";

    String prefix = "cdap-dlp-test";
    String bucketName = String.format("%s-%s", prefix, UUID.randomUUID());
    bucket = storage.create(BucketInfo.of(bucketName));
    bucket.create("test-input.csv", testCSVInput.getBytes(StandardCharsets.UTF_8));

    Schema sourceSchema = Schema.recordOf("etlSchemaBody",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

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
            .put("path", createPath(bucket, "test-input.csv")).build(), null));

    dlpServiceClient = DlpServiceClient.create(
      DlpServiceSettings
        .newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(
          new ByteArrayInputStream(
            getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8)))))
        .build());

    List<InfoType> infoTypes =
      Stream.of("PHONE_NUMBER", "EMAIL_ADDRESS")
        .map(it -> InfoType.newBuilder().setName(it).build())
        .collect(Collectors.toList());

    InspectConfig inspectConfig = InspectConfig.newBuilder().addAllInfoTypes(infoTypes).build();

    inspectTemplate =
      InspectTemplate.newBuilder()
        .setName(INSPECT_TEMPLATE_NAME)
        .setInspectConfig(inspectConfig)
        .build();

    CreateInspectTemplateRequest createInspectTemplateRequest =
      CreateInspectTemplateRequest.newBuilder()
        .setParent(ProjectName.of(getProjectId()).toString())
        .setInspectTemplate(inspectTemplate)
        .build();

    inspectTemplate = dlpServiceClient.createInspectTemplate(createInspectTemplateRequest);

    String templateName = inspectTemplate.getName();
    templateId = templateName.substring(templateName.lastIndexOf('/') + 1);
  }

  @AfterClass
  public static void tearDownClass() {
    DeleteInspectTemplateRequest deleteInspectTemplateRequest =
      DeleteInspectTemplateRequest.newBuilder()
        .setName(inspectTemplate.getName())
        .build();

    dlpServiceClient.deleteInspectTemplate(deleteInspectTemplateRequest);

    dlpServiceClient.close();

    for (Blob blob : bucket.list().iterateAll()) {
      blob.delete();
    }
    bucket.delete(Bucket.BucketSourceOption.metagenerationMatch());
  }

  protected void assertGCSContentsMatch(Bucket bucket, String blobNamePrefix, String content) {
    Set<String> expected = new HashSet<>(Arrays.asList(content.trim().split("\n")));
    String actualContent =
      Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(blobNamePrefix)).iterateAll())
        .stream().map(blob -> new String(blob.getContent(), StandardCharsets.UTF_8))
        .collect(Collectors.joining());
    Set<String> actual = new HashSet<>(Arrays.asList(actualContent.trim().split("\n")));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRedaction() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String fieldsToTransform = objectMapper.writeValueAsString(Collections.singletonList(
      objectMapper.writeValueAsString(new ImmutableMap.Builder<String, Object>()
        .put("fields", "body")
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
          .put("templateId", templateId).build(), null));

    ETLStage sinkStage =
      new ETLStage("GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
        .put("project", getProjectId())
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-redaction")
        .put("path", createPath(bucket, "test-output")).build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(gcsSourceStage)
      .addStage(redactStage)
      .addStage(sinkStage)
      .addConnection(gcsSourceStage.getName(), redactStage.getName())
      .addConnection(redactStage.getName(), sinkStage.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("DLPRedactTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertGCSContentsMatch(bucket, "test-output/",
      "0,alice,#################\n"
        + "1,bob,###############\n"
        + "2,craig,#################\n");
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
          .put("template-id", templateId).build(), null));

    ETLStage sinkStage1 =
      new ETLStage("GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE, new ImmutableMap.Builder<String, String>()
        .put("project", getProjectId())
        .put("format", "csv")
        .put("serviceFilePath", "auto-detect")
        .put("location", "us")
        .put("referenceName", "sink-emails-with-filter")
        .put("path", createPath(bucket, "test-output-sensitive")).build(), null));

    ETLStage sinkStage2 =
      new ETLStage("copy of GCS2", new ETLPlugin("GCS", BatchSink.PLUGIN_TYPE,
        new ImmutableMap.Builder<String, String>()
          .put("project", getProjectId())
          .put("format", "csv")
          .put("serviceFilePath", "auto-detect")
          .put("location", "us")
          .put("referenceName", "sink-emails-with-filter")
          .put("path", createPath(bucket, "test-output-nonsensitive")).build(), null));

    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(gcsSourceStage)
      .addStage(filterStage)
      .addStage(sinkStage1)
      .addStage(sinkStage2)
      .addConnection(gcsSourceStage.getName(), filterStage.getName())
      .addConnection(filterStage.getName(), sinkStage1.getName(), "Sensitive")
      .addConnection(filterStage.getName(), sinkStage2.getName(), "Non-Sensitive")
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("DLPFilterTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertGCSContentsMatch(bucket, "test-output-sensitive/",
      "0,alice,alice@example.com\n"
        + "1,bob,bob@example.com\n"
        + "2,craig,craig@example.com\n");
    assertGCSContentsMatch(bucket, "test-output-nonsensitive/", "");
  }
}
