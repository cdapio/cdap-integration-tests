/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.plugin.common.batch.action.Condition;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Tests reading from GCS (Google Cloud Storage) and writing to GCS from within a Dataproc cluster.
 */
public class GCSTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSTest.class);
  private static final String OUTPUT_BLOB_NAME = "data/output";
  private static final String GCS_BUCKET_DELETE_PLUGIN_NAME = "GCSBucketDelete";
  private static final String GCS_BUCKET_CREATE_PLUGIN_NAME = "GCSBucketCreate";
  private static final String GCS_MOVE_PLUGIN_NAME = "GCSMove";
  private static final String GCS_COPY_PLUGIN_NAME = "GCSCopy";
  private static final String GCS_DONE_FILE_MARKER_PLUGIN_NAME = "GCSDoneFileMarker";
  private static final String SINK_PLUGIN_NAME = "GCS";
  private static final String SOURCE_PLUGIN_NAME = "GCSFile";
  private static final Schema ALL_DT_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("int", Schema.of(Schema.Type.INT)),
    Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("decimal", Schema.decimalOf(5, 4)),
    Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("record", Schema.recordOf(
      "nested",
      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("b", Schema.of(Schema.Type.STRING)))
    ),
    Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
  );
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static Storage storage;
  private List<String> markedForDeleteBuckets;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    storage = StorageOptions.newBuilder()
      .setProjectId(getProjectId())
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8))))
      .build().getService();
  }

  @Override
  protected void innerSetup() throws Exception {
    // wait for artifact containing GCSCopy to load
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId datapipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        List<PluginSummary> plugins =
          artifactClient.getPluginSummaries(datapipelineId, Action.PLUGIN_TYPE, ArtifactScope.SYSTEM);
        return plugins.stream().anyMatch(pluginSummary -> "GCSCopy".equals(pluginSummary.getName()));
      } catch (ArtifactNotFoundException e) {
        // happens if the relevant artifact(s) were not added yet
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
    markedForDeleteBuckets = new ArrayList<>();
  }

  @Override
  protected void innerTearDown() {
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

  private List<String> listBucket(Bucket bucket, String prefix) {
    List<String> blobs = new ArrayList<>();
    for (Blob b: bucket.list(Storage.BlobListOption.prefix(prefix)).iterateAll()) {
      String blobName = b.getName();
      if (!blobName.contains("_temp") && !blobName.contains("SUCCESS") && !blobName.endsWith("/")) {
        blobs.add(b.getName());
      }
    }
    return blobs;
  }

  private void markBucketNameForDelete(String bucketName) {
    markedForDeleteBuckets.add(bucketName);
  }

  private String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  @Test
  public void testGCSCopy() throws Exception {
    testGCSCopy(Engine.MAPREDUCE);
    testGCSCopy(Engine.SPARK);
  }

  private void testGCSCopy(Engine engine) throws Exception {
    String prefix = "cdap-gcs-cp-test";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String bucket2Name = String.format("%s-2-%s", prefix, UUID.randomUUID());
    String bucket3Name = String.format("%s-3-%s", prefix, UUID.randomUUID());
    String bucket4Name = String.format("%s-4-%s", prefix, UUID.randomUUID());

    Bucket bucket1 = createBucket(bucket1Name);
    Bucket bucket2 = createBucket(bucket2Name);
    Bucket bucket3 = createBucket(bucket3Name);
    Bucket bucket4 = createBucket(bucket4Name);

    /*
        Start off with the following objects:

        bucket1/catalog.txt
        bucket1/stats.txt
        bucket1/listings/2018-01-01/l0.txt
        bucket1/listings/2018-01-01/l1.txt
        bucket1/listings/2018-02-01/l2.txt
     */
    String catalogContent = "c";
    String statsContent = "s";
    String l0Content = "l0";
    String l1Content = "l1";
    String l2Content = "l2";
    bucket1.create("catalog.txt", catalogContent.getBytes(StandardCharsets.UTF_8));
    bucket1.create("stats.txt", statsContent.getBytes(StandardCharsets.UTF_8));
    bucket1.create("listings/2018-01-01/l0.txt", l0Content.getBytes(StandardCharsets.UTF_8));
    bucket1.create("listings/2018-01-01/l1.txt", l1Content.getBytes(StandardCharsets.UTF_8));
    bucket1.create("listings/2018-02-01/l2.txt", l2Content.getBytes(StandardCharsets.UTF_8));

    /*
        cp bucket1 bucket2
        should result in:

        bucket2/catalog.txt
        bucket2/stats.txt
     */
    ETLStage cp1 = createCopyStage("cp1", bucket1Name, bucket2Name, false);

    /*
        cp -r bucket1 bucket3
        should result in everything being copied
     */
    ETLStage cp2 = createCopyStage("cp2", bucket1Name, bucket3Name, true);

    /*
        cp bucket1/catalog.txt bucket4/catalog-backup.txt
        should copy the one file
     */
    ETLStage cp3 = createCopyStage("cp3", String.format("%s/catalog.txt", bucket1Name),
                                   String.format("%s/catalog-backup.txt", bucket4Name), false);

    /*
        cp bucket1/catalog.txt bucket4/dir1 when 'dir1' already exists
        should copy the file into bucket4/dir1/catalog.txt
     */
    bucket4.create("dir1/", new byte[] { });
    ETLStage cp4 = createCopyStage("cp4", String.format("%s/catalog.txt", bucket1Name),
                                   String.format("%s/dir1", bucket4Name), false);

    /*
        cp bucket1/catalog.txt bucket4/dir2/
        should copy the file into bucket4/dir2/catalog.txt even though 'dir2' does not yet exist because of the
        ending slash in dir2/
     */
    ETLStage cp5 = createCopyStage("cp5", String.format("%s/catalog.txt", bucket1Name),
                                   String.format("%s/dir2/", bucket4Name), false);

    /*
        cp -r bucket1/listings bucket4/dir3 when 'dir3' does not exist should result in:
        bucket4/dir3/2018-01-01/l0.txt
        bucket4/dir3/2018-01-01/l1.txt
        bucket4/dir3/2018-02-01/l2.txt
     */
    ETLStage cp6 = createCopyStage("cp6", String.format("%s/listings", bucket1Name),
                                   String.format("%s/dir3", bucket4Name), true);

    /*
        cp -r bucket1/listings bucket4/dir3 when 'dir4' already exists should result in:
        bucket4/dir4/listings/2018-01-01/l0.txt
        bucket4/dir4/listings/2018-01-01/l1.txt
        bucket4/dir4/listings/2018-02-01/l2.txt
     */
    bucket4.create("dir4/", new byte[] { });
    ETLStage cp7 = createCopyStage("cp7", String.format("%s/listings", bucket1Name),
                                   String.format("%s/dir4", bucket4Name), true);

    // deploy the pipeline
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .addStage(cp2)
      .addStage(cp3)
      .addStage(cp4)
      .addStage(cp5)
      .addStage(cp6)
      .addStage(cp7)
      .addConnection(cp1.getName(), cp2.getName())
      .addConnection(cp2.getName(), cp3.getName())
      .addConnection(cp3.getName(), cp4.getName())
      .addConnection(cp4.getName(), cp5.getName())
      .addConnection(cp5.getName(), cp6.getName())
      .addConnection(cp6.getName(), cp7.getName())
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSCopyTest" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    /*
        From cp1, bucket 2 should look like:

        bucket2/catalog.txt
        bucket2/stats.txt
     */
    assertGCSContents(bucket2, "catalog.txt", catalogContent);
    assertGCSContents(bucket2, "stats.txt", statsContent);
    Assert.assertNull("directory should not get copied during non-recursive copy", bucket2.get("listings/"));

    /*
        From cp2, bucket 3 should look like:

        bucket3/catalog.txt
        bucket3/stats.txt
        bucket3/listings/2018-01-01/l0.txt
        bucket3/listings/2018-01-01/l1.txt
        bucket3/listings/2018-02-01/l2.txt
     */
    assertGCSContents(bucket3, "catalog.txt", catalogContent);
    assertGCSContents(bucket3, "stats.txt", statsContent);
    assertGCSContents(bucket3, "listings/2018-01-01/l0.txt", l0Content);
    assertGCSContents(bucket3, "listings/2018-01-01/l1.txt", l1Content);
    assertGCSContents(bucket3, "listings/2018-02-01/l2.txt", l2Content);

    /*
        From other copies, bucket 4 should look like:

        bucket4/catalog-backup.txt
        bucket4/dir1/catalog.txt
        bucket4/dir2/catalog.txt
        bucket4/dir3/2018-01-01/l0.txt
        bucket4/dir3/2018-01-01/l1.txt
        bucket4/dir3/2018-02-01/l2.txt
        bucket4/dir4/listings/2018-01-01/l0.txt
        bucket4/dir4/listings/2018-01-01/l1.txt
        bucket4/dir4/listings/2018-02-01/l2.txt
     */
    assertGCSContents(bucket4, "catalog-backup.txt", catalogContent);
    assertGCSContents(bucket4, "dir1/catalog.txt", catalogContent);
    assertGCSContents(bucket4, "dir2/catalog.txt", catalogContent);
    assertGCSContents(bucket4, "dir3/2018-01-01/l0.txt", l0Content);
    assertGCSContents(bucket4, "dir3/2018-01-01/l1.txt", l1Content);
    assertGCSContents(bucket4, "dir3/2018-02-01/l2.txt", l2Content);
    assertGCSContents(bucket4, "dir4/listings/2018-01-01/l0.txt", l0Content);
    assertGCSContents(bucket4, "dir4/listings/2018-01-01/l1.txt", l1Content);
    assertGCSContents(bucket4, "dir4/listings/2018-02-01/l2.txt", l2Content);
  }

  @Test
  public void testGCSMoveNonRecursives() throws Exception {
    testGCSMoveNonRecursive(Engine.MAPREDUCE);
    testGCSMoveNonRecursive(Engine.SPARK);
  }

  private void testGCSMoveNonRecursive(Engine engine) throws Exception {
    String prefix = "cdap-gcs-mv-rec";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String bucket2Name = String.format("%s-2-%s", prefix, UUID.randomUUID());

    Bucket bucket1 = createBucket(bucket1Name);
    Bucket bucket2 = createBucket(bucket2Name);

    bucket1.create("dir/1/1.txt", "1-1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/1/2.txt", "1-2".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/2/1.txt", "2-1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/2/2.txt", "2-2".getBytes(StandardCharsets.UTF_8));
    bucket1.create("1.txt", "1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("2.txt", "2".getBytes(StandardCharsets.UTF_8));

    ETLStage cp1 = createMoveStage("mv1", bucket1Name, bucket2Name, false);

    // deploy the pipeline
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSMoveTest" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    /*
        bucket2 must have only items from bucket1 root
     */
    assertNotExists(bucket2, "dir/1/1.txt");
    assertNotExists(bucket2, "dir/1/2.txt");
    assertNotExists(bucket2, "dir/2/1.txt");
    assertNotExists(bucket2, "dir/2/2.txt");
    assertGCSContents(bucket2, "1.txt", "1");
    assertGCSContents(bucket2, "2.txt", "2");
  }

  @Test
  public void testGCSMoveRecursive() throws Exception {
    testGCSMoveRecursive(Engine.MAPREDUCE);
    testGCSMoveRecursive(Engine.SPARK);
  }

  private void testGCSMoveRecursive(Engine engine) throws Exception {
    String prefix = "cdap-gcs-mv-nonrec";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String bucket2Name = String.format("%s-2-%s", prefix, UUID.randomUUID());

    Bucket bucket1 = createBucket(bucket1Name);
    Bucket bucket2 = createBucket(bucket2Name);

    bucket1.create("dir/1/1.txt", "1-1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/1/2.txt", "1-2".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/2/1.txt", "2-1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/2/2.txt", "2-2".getBytes(StandardCharsets.UTF_8));
    bucket1.create("1.txt", "1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("2.txt", "2".getBytes(StandardCharsets.UTF_8));

    ETLStage cp1 = createMoveStage("mv1", bucket1Name, bucket2Name, true);

    // deploy the pipeline
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSMoveTestRecursive" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    /*
        bucket2 must have exactly same content as bucket1
     */
    assertGCSContents(bucket2, "dir/1/1.txt", "1-1");
    assertGCSContents(bucket2, "dir/1/2.txt", "1-2");
    assertGCSContents(bucket2, "dir/2/1.txt", "2-1");
    assertGCSContents(bucket2, "dir/2/2.txt", "2-2");
    assertGCSContents(bucket2, "1.txt", "1");
    assertGCSContents(bucket2, "2.txt", "2");
  }

  @Test
  public void testCreateFileMarkerOnPipelineSuccess() throws Exception {
    String appIdName = "CreateFileMarkerOnPipelineSuccess";
    String sourceBucketPath = String.format("%s-%s", "source", UUID.randomUUID());
    String destinationBucketPath = String.format("%s-%s", "destination", UUID.randomUUID());
    String recursive = "true";
    String markerFileBlobName = "__SUCCESS";
    String markerFilePath = destinationBucketPath + "/" + markerFileBlobName;
    String runCondition = Condition.SUCCESS.name();

    // create buckets
    Bucket sourceBucket = createBucket(sourceBucketPath);
    Bucket destinationBucket = createBucket(destinationBucketPath); // same as marker filer bucket

    // create blobs at the source bucket
    sourceBucket.create("1.txt", "1".getBytes(StandardCharsets.UTF_8));
    sourceBucket.create("2.txt", "2".getBytes(StandardCharsets.UTF_8));

    // create move plugin
    Map<String, String> movePluginProperties = new ImmutableMap.Builder<String, String>()
      .put("project", "${project}")
      .put("sourcePath", "${source_bucket_path}")
      .put("destPath", "${destination_bucket_path}")
      .put("recursive", "${recursive}")
      .build();

    ETLPlugin moveActionPlugin = new ETLPlugin(
      GCS_MOVE_PLUGIN_NAME,
      Action.PLUGIN_TYPE,
      movePluginProperties,
      GOOGLE_CLOUD_ARTIFACT
    );

    ETLStage moveStage = new ETLStage("move-action-plugin", moveActionPlugin);

    // create marker file plugin
    Map<String, String> markerFileActionProperties = new ImmutableMap.Builder<String, String>()
      .put("project", "${project}")
      .put("path", "${marker_file_path}")
      .put("runCondition", runCondition)
      .put("serviceFilePath", "auto-detect")
      .build();

    ETLPlugin markerFilePostActionPlugin = new ETLPlugin(
      GCS_DONE_FILE_MARKER_PLUGIN_NAME,
      PostAction.PLUGIN_TYPE,
      markerFileActionProperties,
      GOOGLE_CLOUD_ARTIFACT
    );

    ETLStage markerFileStage = new ETLStage("marker-file-post-action-plugin", markerFilePostActionPlugin);

    // deploy the pipeline
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(moveStage)
      .addPostAction(markerFileStage)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app(appIdName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set parameters
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("source_bucket_path", sourceBucketPath);
    args.put("destination_bucket_path", destinationBucketPath);
    args.put("recursive", recursive);
    args.put("marker_file_path", markerFilePath);

    // start the pipeline and wait till it finishes
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED, args);

    // the destinationBucket should have all the content from the sourceBucket plus the __SUCCESS file
    assertGCSContents(destinationBucket, "1.txt", "1");
    assertGCSContents(destinationBucket, "2.txt", "2");
    assertExists(destinationBucket, markerFileBlobName);
  }

  @Test
  public void testCreateFileMarkerOnPipelineFailure() throws Exception {
    String appIdName = "CreateFileMarkerOnPipelineFailure";
    String sourceBucketPath = String.format("%s-%s", "source", UUID.randomUUID());
    String destinationBucketPath = String.format("%s-%s", "destination", UUID.randomUUID());
    String recursive = "true";
    String markerFileBlobName = "__FAILED";
    String markerFilePath = destinationBucketPath + "/" + markerFileBlobName;
    String runCondition = Condition.FAILURE.name();

    // create buckets
    Bucket sourceBucket = createBucket(sourceBucketPath);
    Bucket destinationBucket = createBucket(destinationBucketPath); // same as marker filer bucket

    // create blobs at the source bucket
    sourceBucket.create("1.txt", "1".getBytes(StandardCharsets.UTF_8));
    sourceBucket.create("2.txt", "2".getBytes(StandardCharsets.UTF_8));

    // In order to test whether a __FAILED marker file will get created when the pipeline fails, we need to make this
    // pipeline fail intentionally. To do so, we provide a non-existing-path to 'serviceFilePath' parameter.
    Map<String, String> movePluginProperties = new ImmutableMap.Builder<String, String>()
      .put("project", "${project}")
      .put("sourcePath", "${source_bucket_path}")
      .put("destPath", "${destination_bucket_path}")
      .put("recursive", "${recursive}")
      .put("serviceAccountType", "filePath")
      // 'serviceFilePath' is provided intentionally wrong in order to make the pipeline fail
      .put("serviceFilePath", "/this/path/does/not/exist.json")
      .build();

    ETLPlugin moveActionPlugin = new ETLPlugin(
      GCS_MOVE_PLUGIN_NAME,
      Action.PLUGIN_TYPE,
      movePluginProperties,
      GOOGLE_CLOUD_ARTIFACT
    );

    ETLStage moveStage = new ETLStage("move-action-plugin", moveActionPlugin);

    // In contrast to the move-action-plugin, the marker-file-post-action-plugin should have the correct parameters.
    // This enables creating the marker file successfully.
    Map<String, String> markerFileActionProperties = new ImmutableMap.Builder<String, String>()
      .put("project", "${project}")
      .put("path", "${marker_file_path}")
      .put("runCondition", runCondition)
      .put("serviceFilePath", "auto-detect")
      .build();

    ETLPlugin markerFilePostActionPlugin = new ETLPlugin(
      GCS_DONE_FILE_MARKER_PLUGIN_NAME,
      PostAction.PLUGIN_TYPE,
      markerFileActionProperties,
      GOOGLE_CLOUD_ARTIFACT
    );

    ETLStage markerFileStage = new ETLStage("marker-file-post-action-plugin", markerFilePostActionPlugin);

    // deploy the pipeline
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(moveStage)
      .addPostAction(markerFileStage)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app(appIdName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // set parameters
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("source_bucket_path", sourceBucketPath);
    args.put("destination_bucket_path", destinationBucketPath);
    args.put("recursive", recursive);
    args.put("marker_file_path", markerFilePath);

    // start the pipeline and wait till it fails
    startWorkFlow(appManager, ProgramRunStatus.FAILED, args);

    // the destinationBucket should have only the __FAILED file
    assertExists(destinationBucket, markerFileBlobName);
  }

  @Test
  public void testGSCCreate() throws Exception {
    testGSCCreate(Engine.MAPREDUCE);
    testGSCCreate(Engine.SPARK);
  }

  private void testGSCCreate(Engine engine) throws Exception {
    String prefix = "cdap-gcs-create-test";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String path = String.format("gs://%s/testFolder,gs://%s/testFolder2", bucket1Name, bucket1Name);
    ETLStage cp1 = new ETLStage("gcs-create", new ETLPlugin(GCS_BUCKET_CREATE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                                            ImmutableMap.of("project", getProjectId(),
                                                                            "paths", path,
                                                                            "failIfExists", String.valueOf(true)),
                                                            GOOGLE_CLOUD_ARTIFACT));
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSCreateTest" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // mark possibly created in future bucket for deletion
    markBucketNameForDelete(bucket1Name);
    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    Bucket bucket1 = storage.get(bucket1Name);
    Assert.assertNotNull(String.format("bucket %s does not exist", bucket1Name), bucket1);

    assertExists(bucket1, "testFolder/");
    assertExists(bucket1, "testFolder2/");
  }

  @Test
  public void testGSCDelete() throws Exception {
    testGSCDelete(Engine.MAPREDUCE);
    testGSCDelete(Engine.SPARK);
  }

  private void testGSCDelete(Engine engine) throws Exception {
    String prefix = "cdap-gcs-delete-test";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());

    Bucket bucket1 = createBucket(bucket1Name);

    bucket1.create("dir/1.txt", "1".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/2.txt", "2".getBytes(StandardCharsets.UTF_8));
    bucket1.create("dir/3.txt", "3".getBytes(StandardCharsets.UTF_8));

    String paths = String.join(",",
                               createPath(bucket1, "dir/1.txt"),
                               createPath(bucket1, "dir/3.txt"));


    ETLStage cp1 = new ETLStage("gcs-delete", new ETLPlugin(GCS_BUCKET_DELETE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                                            ImmutableMap.of("project", getProjectId(),
                                                                            "paths", paths),
                                                            GOOGLE_CLOUD_ARTIFACT));
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSDeleteTest" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    assertNotExists(bucket1, "dir/1.txt");
    assertNotExists(bucket1, "dir/3.txt");
    assertGCSContents(bucket1, "dir/2.txt", "2");
  }


  private void assertGCSContents(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
    Assert.assertEquals(content, new String(blob.getContent(), StandardCharsets.UTF_8));
  }

  private void assertBlobContains(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertTrue(new String(blob.getContent(), StandardCharsets.UTF_8).contains(content));
  }

  private void assertBlobNotContains(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertFalse(new String(blob.getContent(), StandardCharsets.UTF_8).contains(content));
  }

  private void assertNotExists(Bucket bucket, String blobName) {
    Blob blob = bucket.get(blobName);
    if (blob != null) {
      Assert.assertFalse(String.format("%s in %s exists but must not", blobName, bucket.getName()), blob.exists());
    }
  }

  private void assertExists(Bucket bucket, String blobName) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
  }

  private ETLStage createCopyStage(String name, String src, String dest, boolean recursive) {
    return new ETLStage(name, new ETLPlugin(GCS_COPY_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                            ImmutableMap.of("project", getProjectId(),
                                                            "sourcePath", src,
                                                            "destPath", dest,
                                                            "recursive", String.valueOf(recursive)),
                                            GOOGLE_CLOUD_ARTIFACT));
  }

  private ETLStage createMoveStage(String name, String src, String dest, boolean recursive) {
    return new ETLStage(name, new ETLPlugin(GCS_MOVE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                            ImmutableMap.of("project", getProjectId(),
                                                            "sourcePath", src,
                                                            "destPath", dest,
                                                            "recursive", String.valueOf(recursive)),
                                            GOOGLE_CLOUD_ARTIFACT));
  }
  
  @Test
  public void testAllTypes() throws Exception {
    testAllTypes(Engine.MAPREDUCE);
    testAllTypes(Engine.SPARK);
  }

  private void testAllTypes(Engine engine) throws Exception {
    String bucketName = "co-cask-test-bucket-" + System.currentTimeMillis();
    Bucket bucket = createBucket(bucketName);
    String inputBlobName = "gcs-types/" + engine + "/test.avro";
    String outputBlobName = "output/" + engine + "/gcs-types/json";

    String schema = ALL_DT_SCHEMA.toString();

    LocalDate date = LocalDate.now();
    int dateExpected = Math.toIntExact(date.toEpochDay());
    LocalTime time = LocalTime.now();
    long timeExpected = TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay());
    ZonedDateTime timestamp = ZonedDateTime.now();
    long timestampExpected = Math.addExact(
      TimeUnit.SECONDS.toMicros(timestamp.toInstant().getEpochSecond()),
      TimeUnit.NANOSECONDS.toMicros(timestamp.toInstant().getNano())
    );

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
    org.apache.avro.Schema childSchema = avroSchema.getField("record").schema();

    GenericRecord childRecord = new GenericData.Record(childSchema);
    childRecord.put("a", "a value");
    childRecord.put("b", "b value");


    GenericRecord record1 = new GenericData.Record(avroSchema);
    record1.put("string", "object1");
    record1.put("boolean", false);
    record1.put("bytes", ByteBuffer.wrap("abc".getBytes()));
    record1.put("int", 123);
    record1.put("double", 123.123);
    record1.put("float", 123.123f);
    record1.put("long", 123456789L);
    record1.put("decimal", ByteBuffer.wrap(new BigInteger("11234").toByteArray()));
    record1.put("array", Arrays.asList("element1", "element2"));
    record1.put("map", new HashMap<String, String>() {{
      put("key1", "value1");
      put("key2", "value2");
    }});
    record1.put("union", "string union value");
    record1.put("record", childRecord);
    record1.put("date", dateExpected);
    record1.put("time", timeExpected);
    record1.put("timestamp", timestampExpected);

    GenericRecord record2 = new GenericData.Record(avroSchema);
    record2.put("string", "object2");
    record2.put("boolean", true);
    record2.put("bytes", ByteBuffer.wrap("cbd".getBytes()));
    record2.put("int", 321);
    record2.put("double", 321.321);
    record2.put("float", 321.321f);
    record2.put("long", 987654321L);
    record2.put("decimal", ByteBuffer.wrap(new BigInteger("43211").toByteArray()));
    record2.put("array", Arrays.asList("element1", "element2"));
    record2.put("map", new HashMap<String, String>() {{
      put("key1", "value1");
      put("key2", "value2");
    }});
    record2.put("union", 123);
    record2.put("record", childRecord);
    record2.put("date", dateExpected);
    record2.put("time", timeExpected);
    record2.put("timestamp", timestampExpected);

    File avroFile = temporaryFolder.newFile();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, avroFile);
    dataFileWriter.append(record1);
    dataFileWriter.append(record2);
    dataFileWriter.close();

    bucket.create(inputBlobName, Files.toByteArray(avroFile));

    ETLStage source = new ETLStage("GCSSourceStage",
                                   new ETLPlugin(SOURCE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   "schema", schema,
                                                   "format", "avro",
                                                   "referenceName", "gcs-input",
                                                   "project", getProjectId(),
                                                   "path", createPath(bucket, inputBlobName)),
                                                 GOOGLE_CLOUD_ARTIFACT));

    ETLStage sink = new ETLStage("GCSSinkStage", new ETLPlugin(SINK_PLUGIN_NAME,
                                                               BatchSink.PLUGIN_TYPE,
                                                               ImmutableMap.of(
                                                                 "path", createPath(bucket, outputBlobName),
                                                                 "format", "json",
                                                                 "project", getProjectId(),
                                                                 "referenceName", "gcs-output"),
                                                               GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("GCSToGCS" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    Gson gson = new Gson();
    List<DataTypesRecord> resultingObjects = getResultBlobsContent(bucket, outputBlobName).stream()
      .flatMap(content -> Arrays.stream(content.split("\\r?\\n"))
        .map(record -> gson.fromJson(record, DataTypesRecord.class)))
      .sorted(Comparator.comparing(o -> o.string))
      .collect(Collectors.toList());

    Assert.assertEquals(2, resultingObjects.size());

    DataTypesRecord object1 = resultingObjects.get(0);
    DataTypesRecord object2 = resultingObjects.get(1);

    Assert.assertFalse(object1.booleanField);
    Assert.assertTrue(object2.booleanField);

    Assert.assertArrayEquals("abc".getBytes(), object1.bytes);
    Assert.assertArrayEquals("cbd".getBytes(), object2.bytes);

    // we can use BigInteger here, scale not important here since it is stored in schema.
    Assert.assertArrayEquals(new BigInteger("11234").toByteArray(), object1.decimal);
    Assert.assertArrayEquals(new BigInteger("43211").toByteArray(), object2.decimal);

    Assert.assertEquals(123, object1.intField);
    Assert.assertEquals(321, object2.intField);

    Assert.assertEquals(123.123, object1.doubleField, 0.00001);
    Assert.assertEquals(321.321, object2.doubleField, 0.00001);
    Assert.assertEquals(123.123f, object1.floatField, 0.00001);
    Assert.assertEquals(321.321f, object2.floatField, 0.00001);
    Assert.assertEquals(123456789L, object1.longField);
    Assert.assertEquals(987654321L, object2.longField);

    Assert.assertEquals("string union value", object1.union);
    // Gson by default deserializing numbers to double
    Assert.assertEquals(123d, (double) object2.union, 0.00001);

    Assert.assertEquals("value1", object1.map.get("key1"));
    Assert.assertEquals("value2", object1.map.get("key2"));
    Assert.assertEquals("a value", object1.record.a);
    Assert.assertEquals("b value", object1.record.b);

    Assert.assertTrue(object1.array.contains("element1"));
    Assert.assertTrue(object1.array.contains("element2"));

    // it is okay to compare values ignoring logical type
    Assert.assertEquals(dateExpected, object1.date);
    Assert.assertEquals(timestampExpected, object1.timestamp);
    Assert.assertEquals(timeExpected, object1.time);
  }

  @Test
  public void testGcsSourceFormats() throws Exception {
    testGcsSourceFormats(Engine.MAPREDUCE);
    testGcsSourceFormats(Engine.SPARK);
  }

  public void testGcsSourceFormats(Engine engine) throws Exception {
    String bucketName = "cask-gcs-formats-" + UUID.randomUUID().toString();
    Bucket bucket = createBucket(bucketName);

    Schema schema = Schema.recordOf("customer",
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    String line1 = "1,Marilyn Hawkins,mhawkins0@example.com";
    String line2 = "2,Terry Perez,tperez1@example.com";
    String line3 = "3,Jack Ferguson,jferguson2@example.com";
    String inputPath = "input";
    bucket.create(inputPath, String.join("\n", Arrays.asList(line1, line2, line3)).getBytes(StandardCharsets.UTF_8));

    String suffix = UUID.randomUUID().toString();
    /*
      First pipeline reads from GCS and writes to one GCS sink per format

                  |--> avro
        source ---|
                  |--> blob
                  |
                  ...
                  |
                  |--> tsv
     */
    ETLStage source = new ETLStage("source",
                                   new ETLPlugin(SOURCE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   "schema", schema.toString(),
                                                   "format", "csv",
                                                   "referenceName", "gcs-input",
                                                   "project", getProjectId(),
                                                   "path", createPath(bucket, inputPath)),
                                                 GOOGLE_CLOUD_ARTIFACT));

    List<String> formats = Arrays.asList("avro", "csv", "delimited", "json", "parquet", "tsv");
    ETLBatchConfig.Builder pipelineConfig = ETLBatchConfig.builder().addStage(source);
    for (String format : formats) {
      String path = String.format("%s/%s/%s", createPath(bucket, OUTPUT_BLOB_NAME), format, suffix);
      ETLStage sink = new ETLStage(format, createSinkPlugin(format, path, schema));
      pipelineConfig.addStage(sink).addConnection(source.getName(), sink.getName());
    }

    pipelineConfig.setEngine(engine);

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(pipelineConfig.build());
    ApplicationId appId = TEST_NAMESPACE.app("GCSFormatSinks" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    /*
      The second pipeline reads from each of those formats and writes to GCS.

      avro --|
             |
      csv ---|
             |---> sink
      ...    |
             |
      tsv ---|

      data looks like
      id,first,last,email,address,city,state,zip
      1,Marilyn,Hawkins,mhawkins0@ted.com,238 Melvin Way,Palo Alto,CA,94302
     */
    ETLStage sink = new ETLStage("sink", createSinkPlugin("csv", createPath(bucket, "output"), schema));
    pipelineConfig = ETLBatchConfig.builder().addStage(sink);
    for (String format : formats) {
      String path = String.format("%s/%s", createPath(bucket, OUTPUT_BLOB_NAME), format);
      source = createSourceStage(format, path, String.format(".*/%s/.*", suffix), schema);
      pipelineConfig.addStage(source).addConnection(source.getName(), sink.getName());
    }

    pipelineConfig.setEngine(engine);

    appRequest = getBatchAppRequestV2(pipelineConfig.build());
    appId = TEST_NAMESPACE.app("GCSFormatSources" + engine);
    appManager = deployApplication(appId, appRequest);
    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    Map<String, Integer> lineCounts = new HashMap<>();
    List<String> results = getResultBlobsContent(bucket, "output");
    for (String result : results) {
      for (String line : result.split("\n")) {
        lineCounts.putIfAbsent(line, 0);
        lineCounts.put(line, lineCounts.get(line) + 1);
      }
    }
    Map<String, Integer> expected = new HashMap<>();
    expected.put(line1, formats.size());
    expected.put(line2, formats.size());
    expected.put(line3, formats.size());
    Assert.assertEquals(expected, lineCounts);
  }

  @Test
  public void testSchemaDetectionOnSingleFile() throws Exception {
    // source bucket
    String sourceBucketName = "source-schema-detection-" + UUID.randomUUID().toString();
    Bucket sourceBucket = createBucket(sourceBucketName);
    String blobSourcePath = createPath(sourceBucket, "authors.csv");

    // sink bucket
    String sinkBucketName = "sink-schema-detection-" + UUID.randomUUID().toString();
    Bucket sinkBucket = createBucket(sinkBucketName);
    String blobSinkPath = createPath(sinkBucket, "output");

    // a single blob
    String header = "Name;Surname;Age;BooksPublished";
    String dataOne = "John;Doe;21;3";
    String dataTwo = "Alice;Green;20;5";
    String content = String.join("\n", Arrays.asList(header, dataOne, dataTwo));
    sourceBucket.create("authors.csv", content.getBytes(StandardCharsets.UTF_8));

    // Schema is not passed (ie. schema is null) in the GCS source plugin so the automatic schema detection is triggered
    Map<String, String> gcsSourcePluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "source-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", blobSourcePath)
      .put("format", "delimited")
      .put("delimiter", ";")
      .put("sampleSize", "1000")
      .put("skipHeader", "true")
      .put("override", "BooksPublished:string")
      .build();

    ETLPlugin gcsSourcePlugin = new ETLPlugin(SOURCE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, gcsSourcePluginParams,
                                              GOOGLE_CLOUD_ARTIFACT);
    ETLStage source = new ETLStage("source", gcsSourcePlugin);

    // gcs sink plugin
    Map<String, String> gcsSinkPluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "sink-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", blobSinkPath)
      .put("format", "json")
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
    List<String> existingBlobNames = listBucket(sinkBucket, "output");

    // Value 20 is of integer data type. Hence no quotes around the number.
    Assert.assertTrue(existingBlobNames.size() > 0);
    String authorsBlobName = existingBlobNames.get(0);
    assertBlobContains(sinkBucket, authorsBlobName, "20");

    // The BooksPublished data type is manually set to String. Thus it contains string values rather than integers.
    assertBlobContains(sinkBucket, authorsBlobName, "\"3\"");
  }

  @Test
  public void testSchemaDetectionOnMultipleFilesWithDifferentSchema() throws Exception {
    /*
    The CSV Automated Schema Detection plugin assumes that all files/blobs residing in a directory have the same
    schema. When the plugin runs, only the first listed file will get investigated for schema detection. After the
    schema gets detected, the plugin continues running (in this test case GCS Source Plugin sources the data from
    the given bucket. The plugin will throw an error when reading the files with different schemas (due to schema
    not matching the current file structure), hence this test case is destined to fail.
     */

    testSchemaDetectionOnMultipleFilesWithDifferentSchema("true");
    testSchemaDetectionOnMultipleFilesWithDifferentSchema("false");
  }

  private void testSchemaDetectionOnMultipleFilesWithDifferentSchema(String skipHeader) throws Exception {
    //Skip writing the header in test blob if skipHeader will be set to false
    boolean addHeader = Boolean.parseBoolean(skipHeader);
    // source bucket
    String sourceBucketName = "source-schema-detection-" + UUID.randomUUID().toString();
    Bucket sourceBucket = createBucket(sourceBucketName);
    String sourcePath = createPath(sourceBucket, ""); // The whole path is scanned

    // sink bucket
    String sinkBucketName = "sink-schema-detection-" + UUID.randomUUID().toString();
    Bucket sinkBucket = createBucket(sinkBucketName);
    String blobSinkPath = createPath(sinkBucket, "output");

    // The first blob
    String authorHeaders = "Name;Surname;Age";
    String author1 = "John;Doe;35";
    String author2 = "Alice;Green;28";
    String authorsBlobContent = String.join("\n", addHeader ?
      Arrays.asList(authorHeaders, author1, author2) :
      Arrays.asList(author1, author2));
    sourceBucket.create("authors.csv", authorsBlobContent.getBytes(StandardCharsets.UTF_8));

    // The second blob with a different schema
    String bookHeaders = "Title;Year;Price";
    String book1 = "Year of Jupyter;2020;19.99";
    String book2 = "The return of Avalon;2019;17.99";
    String booksBlobContent = String.join("\n", addHeader ?
      Arrays.asList(bookHeaders, book1, book2) :
      Arrays.asList(book1, book2));
    sourceBucket.create("books.csv", booksBlobContent.getBytes(StandardCharsets.UTF_8));

    // gcs source plugin
    Map<String, String> gcsSourcePluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "source-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", sourcePath)
      .put("format", "delimited")
      .put("delimiter", ";")
      .put("sampleSize", "1000")
      .put("skipHeader", skipHeader)
      .build();

    ETLPlugin gcsSourcePlugin = new ETLPlugin(SOURCE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, gcsSourcePluginParams,
                                              GOOGLE_CLOUD_ARTIFACT);
    ETLStage source = new ETLStage("source", gcsSourcePlugin);

    // gcs sink plugin
    Map<String, String> gcsSinkPluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "sink-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", blobSinkPath)
      .put("format", "json")
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
    startWorkFlow(appManager, ProgramRunStatus.FAILED); // the test is destined to fail

    List<String> existingBlobNames = listBucket(sinkBucket, "output");

    // Since the pipeline fails, no blobs will get written in the destination bucket
    Assert.assertEquals(existingBlobNames.size(), 0);
  }

  @Test
  public void testSchemaDetectionOfMultipleFilesWithDifferentSchemaWithFileFilterRegexProvided() throws Exception {
    /*
    In this test case, the plugin will filter out all the files from the source bucket that do not match the given file
    filter regex ".+authors.*". So from authors-part-1.csv, authors-part-2.csv and books.csv, only
    authors-part-1.csv and authors-part-2.csv will get further passed in the pipeline.
     */

    String sourceBucketName = "source-schema-detection-" + UUID.randomUUID().toString();
    Bucket sourceBucket = createBucket(sourceBucketName);

    String sinkBucketName = "sink-schema-detection-" + UUID.randomUUID().toString();
    Bucket sinkBucket = createBucket(sinkBucketName);

    String sourcePath = createPath(sourceBucket, ""); // The whole path is scanned
    String blobSinkPath = createPath(sinkBucket, "output");

    // The first blob
    String authorHeaders = "Name;Surname;Age";
    String author1 = "John;Doe;35";
    String author2 = "Alice;Green;28";
    String authorsBlobContent = String.join("\n", Arrays.asList(authorHeaders, author1, author2));
    sourceBucket.create("authors-part-1.csv", authorsBlobContent.getBytes(StandardCharsets.UTF_8));

    // The second blob with the same schema as the first one
    String otherAuthorHeaders = "Name;Surname;Age";
    String otherAuthor1 = "Brian;Alexander;23";
    String otherAuthor2 = "Sally;Richards;21";
    String otherAuthorsBlobContent = String.join("\n", Arrays.asList(otherAuthorHeaders, otherAuthor1, otherAuthor2));
    sourceBucket.create("authors-part-2.csv", otherAuthorsBlobContent.getBytes(StandardCharsets.UTF_8));

    // The third blob with a different schema
    String bookHeaders = "Title;Year;Price";
    String book1 = "Year of Jupyter;2020;19.99";
    String book2 = "The return of Avalon;17.99";
    String booksBlobContent = String.join("\n", Arrays.asList(bookHeaders, book1, book2));
    sourceBucket.create("books.csv", booksBlobContent.getBytes(StandardCharsets.UTF_8));

    // Schema is not passed (ie. schema is null) in the GCS source plugin so the automatic schema detection is triggered
    Map<String, String> gcsSourcePluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "source-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", sourcePath)
      .put("format", "delimited")
      .put("delimiter", ";")
      .put("sampleSize", "1000")
      .put("skipHeader", "true")
      .put("fileRegex", ".+authors.*")
      .build();

    ETLPlugin gcsSourcePlugin = new ETLPlugin(
      SOURCE_PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      gcsSourcePluginParams,
      GOOGLE_CLOUD_ARTIFACT
    );

    ETLStage source = new ETLStage("source", gcsSourcePlugin);

    // gcs sink plugin
    Map<String, String> gcsSinkPluginParams = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "sink-gcs-schema-detection")
      .put("project", getProjectId())
      .put("path", blobSinkPath)
      .put("format", "json")
      .build();

    ETLPlugin gcsSinkPlugin = new ETLPlugin(
      SINK_PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      gcsSinkPluginParams,
      GOOGLE_CLOUD_ARTIFACT
    );

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

    List<String> existingBlobNames = listBucket(sinkBucket, "output");
    Assert.assertNotEquals(existingBlobNames.size(), 0);

    // check if the sink bucket contains data from both authors-part-1.csv & authors-part-2.csv
    assertBlobContains(sinkBucket, existingBlobNames.get(0), "John");
    assertBlobContains(sinkBucket, existingBlobNames.get(0), "Sally");

    // check if the books file is filtered out
    assertBlobNotContains(sinkBucket, existingBlobNames.get(0), "Year of Jupyter");

    // check if int data type was inferred properly
    assertBlobContains(sinkBucket, existingBlobNames.get(0), "35");
    assertBlobNotContains(sinkBucket, existingBlobNames.get(0), "\"35\"");
  }

  private ETLStage createSourceStage(String format, String path, String regex, Schema schema) {
    return new ETLStage(format, new ETLPlugin(SOURCE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                              new ImmutableMap.Builder<String, String>()
                                                .put("path", path)
                                                .put("format", format)
                                                .put("project", getProjectId())
                                                .put("referenceName", format)
                                                .put("fileRegex", regex)
                                                .put("recursive", "true")
                                                .put("schema", schema.toString()).build(),
                                              GOOGLE_CLOUD_ARTIFACT));
  }

  private ETLPlugin createSinkPlugin(String format, String path, Schema schema) {
    return new ETLPlugin(SINK_PLUGIN_NAME, BatchSink.PLUGIN_TYPE,
                         ImmutableMap.of(
                           "path", path,
                           "format", format,
                           "project", getProjectId(),
                           "referenceName", format,
                           "schema", schema.toString()),
                         GOOGLE_CLOUD_ARTIFACT);
  }

  static class DataTypesRecord {
    static class NestedRecord {
      String a;
      String b;
    }

    String string;

    @SerializedName("boolean")
    boolean booleanField;

    @SerializedName("double")
    double doubleField;

    @SerializedName("long")
    long longField;

    @SerializedName("float")
    float floatField;

    @SerializedName("int")
    int intField;

    byte[] bytes;

    // decimal field written as bytes to json
    byte[] decimal;

    List<String> array;

    Map<String, String> map;

    Object union;

    NestedRecord record;

    int date;
    long time;
    long timestamp;
  }

  /**
   * Checks if given path contains _SUCCESS marker of successfully finished pipeline and returns list of content
   * of every artifact in path.
   */
  private List<String> getResultBlobsContent(Bucket bucket, String path) {
    String successFile = path + "/_SUCCESS";
    assertExists(bucket, successFile);

    return StreamSupport.stream(bucket.list().iterateAll().spliterator(), false)
      .filter(blob -> blob.getName().startsWith(path + "/") && !successFile.equals(blob.getName()))
      .map(GCSTest::blobContentToString)
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Reads content of Blob to String.
   */
  private static String blobContentToString(Blob blob) {
    byte[] content = blob.getContent();
    String contentString = Preconditions.checkNotNull(Bytes.toString(content));
    if (!contentString.isEmpty()) {
      return contentString;
    }
    return null;
  }

}
