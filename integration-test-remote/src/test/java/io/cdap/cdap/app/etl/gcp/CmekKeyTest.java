package io.cdap.cdap.app.etl.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.LocationName;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class CmekKeyTest extends DataprocETLTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(GCSTest.class);
  private static final String SINK_PLUGIN_NAME = "GCS";
  private static final String SOURCE_PLUGIN_NAME = "GCSFile";
  private static final String projectNumber = "16626541024";
  private static final String location = "us-east1";
  private static final String cmekKey =
    "projects/cask-audi-clusters/locations/us-east1/keyRings/test-key/cryptoKeys/test-key-1";
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static Storage storage;
  private static String projectId;
  private static KeyManagementServiceClient keyManagementServiceClient;
  private List<String> markedForDeleteBuckets;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    projectId = getProjectId();
    storage = StorageOptions.newBuilder()
      .setProjectId(getProjectId())
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8))))
      .build().getService();
    //keyManagementServiceClient = KeyManagementServiceClient.create();
  }
  
  @Override
  protected void innerSetup() throws Exception {
    markedForDeleteBuckets = new ArrayList<>();
  }

  private KeyRing createKeyRing() {
    LocationName locationName = LocationName.of(projectId, location);
    KeyRing keyRing = KeyRing.newBuilder().build();
    KeyRing createdKeyRing = keyManagementServiceClient.createKeyRing(locationName, "test-key", keyRing);
    return createdKeyRing;
  }

  @Override
  protected void innerTearDown() throws Exception {
    for (String bucketName : markedForDeleteBuckets) {
      try {
        Bucket bucket = storage.get(bucketName);
        if (bucket != null) {
          deleteBucket(bucket);
        }
      } catch (RuntimeException e) {
        LOG.error("Unable to delete GCS bucket {}", bucketName, e);
      }
    }
  }

  /**
   * Create a bucket that will be automatically deleted when the test completes.
   */
  private Bucket createBucket(String name) {
    LOG.info("Creating bucket {}", name);
    Bucket bucket = storage.create(BucketInfo.of(name));
    LOG.info("Created bucket {}", name);
    markedForDeleteBuckets.add(name);
    return bucket;
  }

  private void deleteBucket(Bucket bucket) {
    for (Blob blob : bucket.list().iterateAll()) {
      LOG.info("Deleting blob {}", blob);
      blob.delete();
    }
    LOG.info("Deleting bucket {}", bucket);
    bucket.delete(Bucket.BucketSourceOption.metagenerationMatch());
  }

  private void assertGCSContents(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
    Assert.assertEquals(content, new String(blob.getContent(), StandardCharsets.UTF_8));
  }

  private String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  @Test
  public void testMultiplePluginsWithDifferentCMEKKeys() throws Exception {
    String prefix = "cdap-gcp-cmek-test";
    String sourceBucketName = String.format("%s-source-%s", prefix, UUID.randomUUID());
    String sinkBucketName = String.format("%s-sink-%s", prefix, UUID.randomUUID());
    String sinkBucketPath = String.format("gs://%s/output", sinkBucketName);
    Bucket bucket1 = createBucket(sourceBucketName);
    String catalogContent = "c";
    bucket1.create("catalog.txt", catalogContent.getBytes(StandardCharsets.UTF_8));
    Map<String, String> gcsSourcePluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "source-gcs")
      .put("useConnection", "false")
      .put("project", projectId)
      .put("serviceAccountType", "filePath")
      .put("serviceFilePath", "auto-detect")
      .put("path", createPath(bucket1, "catalog.txt"))
      .put("format", "text")
      .put("sampleSize", "1000")
      .put("filenameOnly", "false")
      .put("recursive", "false")
      .put("encrypted", "false")
      .build();

    ETLPlugin gcsSourcePlugin = new ETLPlugin(SOURCE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, gcsSourcePluginParams,
                                              GOOGLE_CLOUD_ARTIFACT);
    ETLStage source = new ETLStage("source", gcsSourcePlugin);
    Map<String, String> gcsSinkPluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "sink-gcs")
      .put("project", projectId)
      .put("serviceAccountType", "filePath")
      .put("serviceFilePath", "auto-detect")
      .put("path", sinkBucketPath)
      .put("format", "csv")
      .put("contentType", "application/octet-stream")
      .put("cmekKey", cmekKey)
      .put("location", location)
      .build();

    ETLPlugin gcsSinkPlugin = new ETLPlugin(SINK_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, gcsSinkPluginParams,
                                            GOOGLE_CLOUD_ARTIFACT);
    ETLStage sink = new ETLStage("sink", gcsSinkPlugin);
    ETLBatchConfig pipelineConfig = ETLBatchConfig
      .builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(pipelineConfig);
    String appName = "GCS_Source_Sink_" + UUID.randomUUID().toString().replace("-", "_");
    ApplicationId appId = TEST_NAMESPACE.app(appName);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);
    markedForDeleteBuckets.add(sinkBucketName);
    Bucket sinkBucket = storage.get(sinkBucketName);
    String sinkBucketKmsKey = sinkBucket.getDefaultKmsKeyName();
    LOG.info("sink bucket created with KMS Key = " + sinkBucketKmsKey);
    Assert.assertEquals(sinkBucketKmsKey, cmekKey);
  }
}
