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

package io.cdap.cdap.app.etl;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
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
  private static final String INPUT_BLOB_NAME = "data/input/customers.csv";
  private static final String OUTPUT_BLOB_NAME = "data/output";
  private static final String GCS_BUCKET_DELETE_PLUGIN_NAME = "GCSBucketDelete";
  private static final String GCS_BUCKET_CREATE_PLUGIN_NAME = "GCSBucketCreate";
  private static final String GCS_MOVE_PLUGIN_NAME = "GCSMove";
  private static final String GCS_COPY_PLUGIN_NAME = "GCSCopy";
  private static final String SINK_PLUGIN_NAME = "GCS";
  private static final String SOURCE_PLUGIN_NAME = "GCSFile";
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

  private void markBucketNameForDelete(String bucketName) {
    markedForDeleteBuckets.add(bucketName);
  }

  private String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  @Test
  public void testGCSCopy() throws Exception {
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
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSCopyTest");
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
  public void testGCSMoveNonRecursive() throws Exception {
    String prefix = "cdap-gcs-mv-rec";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String bucket2Name = String.format("%s-2-%s", prefix, UUID.randomUUID());
    String bucket3Name = String.format("%s-3-%s", prefix, UUID.randomUUID());
    String bucket4Name = String.format("%s-4-%s", prefix, UUID.randomUUID());

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
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSMoveTest");
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
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSMoveTestRecursive");
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
  public void testGSCCreate() throws Exception {
    String prefix = "cdap-gcs-create-test";
    String bucket1Name = String.format("%s-1-%s", prefix, UUID.randomUUID());
    String path = String.format("gs://%s/testFolder,gs://%s/testFolder2", bucket1Name, bucket1Name);
    ETLStage cp1 = new ETLStage("gcs-create", new ETLPlugin(GCS_BUCKET_CREATE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                                            ImmutableMap.of("projectId", getProjectId(),
                                                                            "paths", path,
                                                                            "failIfExists", String.valueOf(true))));
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSCreateTest");
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
                                                            ImmutableMap.of("projectId", getProjectId(),
                                                                            "paths", paths)));
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(cp1)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(config);
    ApplicationId appId = TEST_NAMESPACE.app("GCSDeleteTest");
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
                                            ImmutableMap.of("projectId", getProjectId(),
                                                            "sourcePath", src,
                                                            "destPath", dest,
                                                            "recursive", String.valueOf(recursive))));
  }

  private ETLStage createMoveStage(String name, String src, String dest, boolean recursive) {
    return new ETLStage(name, new ETLPlugin(GCS_MOVE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                            ImmutableMap.of("projectId", getProjectId(),
                                                            "sourcePath", src,
                                                            "destPath", dest,
                                                            "recursive", String.valueOf(recursive))));
  }

  @Test
  public void testAllTypes() throws Exception {
    String bucketName = "co-cask-test-bucket-" + System.currentTimeMillis();
    Bucket bucket = createBucket(bucketName);
    String inputBlobName = "gcs-types/test.avro";
    String outputBlobName = "output/gcs-types/json";

    bucket.create(inputBlobName, Resources.toByteArray(Resources.getResource("gcs-types/test.avro")));

    String schema = Resources.toString(Resources.getResource("gcs-types/schema.json"), Charsets.UTF_8);

    ETLStage source = new ETLStage("GCSSourceStage",
                                   new ETLPlugin(SOURCE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   "schema", schema,
                                                   "format", "avro",
                                                   "referenceName", "gcs-input",
                                                   "projectId", getProjectId(),
                                                   "path", createPath(bucket, inputBlobName))));

    ETLStage sink = new ETLStage("GCSSinkStage", new ETLPlugin(SINK_PLUGIN_NAME,
                                                               BatchSink.PLUGIN_TYPE,
                                                               ImmutableMap.of(
                                                                 "path", createPath(bucket, outputBlobName),
                                                                 "format", "json",
                                                                 "projectId", getProjectId(),
                                                                 "referenceName", "gcs-output")));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("GCSToGCS");
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
  }

  @Test
  public void testGCSToGCS() throws Exception {
    String bucketName = "co-cask-test-bucket-" + System.currentTimeMillis();
    Bucket bucket = createBucket(bucketName);

    try (InputStream is = new FileInputStream("src/test/resources/customers.csv")) {
      bucket.create(INPUT_BLOB_NAME, is);
    }

    Map<String, String> idToRowMap;
    try (InputStream is = new FileInputStream("src/test/resources/customers.csv")) {
      String result = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
      idToRowMap = parseIdToRow(result, false);
    }

    String sourceSchema = "{\"type\":\"record\",\"name\":\"etlSchemaBody\"," +
      "\"fields\":[{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"body\",\"type\":\"string\"}]}";
    ETLStage source = new ETLStage("GCSSourceStage",
                                   new ETLPlugin(SOURCE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   "schema", sourceSchema,
                                                   "format", "text",
                                                   "referenceName", "gcs-input",
                                                   "projectId", getProjectId(),
                                                   "path", createPath(bucket, INPUT_BLOB_NAME))));

    ETLStage sink = new ETLStage("FileSinkStage", new ETLPlugin(SINK_PLUGIN_NAME,
                                                                BatchSink.PLUGIN_TYPE,
                                                                ImmutableMap.of(
                                                                  "path", createPath(bucket, OUTPUT_BLOB_NAME),
                                                                  "format", "delimited",
                                                                  "projectId", getProjectId(),
                                                                  "referenceName", "gcs-output")));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("GCSToGCS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    startWorkFlow(appManager, ProgramRunStatus.COMPLETED);

    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + source.getName() + ".records.out", 101, 10);
    checkMetric(tags, "user." + sink.getName() + ".records.in", 101, 10);

    String successFile = OUTPUT_BLOB_NAME + "/_SUCCESS";
    Assert.assertNotNull(bucket.get(successFile));

    Map<String, String> retrievedIdToRowMap = getResultBlobsContent(bucket, OUTPUT_BLOB_NAME).stream()
      .flatMap(content -> parseIdToRow(content, true).entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // check that the output content is equivalent to the input content. Output content files are not guaranteed to be
    // split the same way, so we put the data into a map before checking equality.
    Assert.assertEquals(idToRowMap, retrievedIdToRowMap);
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

  /**
   * Parses a string, whose content is a newline-separated list of rows.
   * The rows are comma-separated, where the first value is the id.
   * The first column may represent file offset, in which case we will skip it.
   */
  private Map<String, String> parseIdToRow(String content, boolean skipFirstColumn) {
    Map<String, String> idToRowMap = new HashMap<>();
    String[] rows = content.split("\n");

    // we skip the first row, because it is the column headers
    for (int idx = 1; idx < rows.length; idx++) {
      String row = rows[idx];
      if (row.isEmpty()) {
        continue;
      }
      if (skipFirstColumn) {
        row = row.split(",", 2)[1];
      }
      String[] rowSplit = row.split(",", 2);
      idToRowMap.put(rowSplit[0], row);
    }
    return idToRowMap;
  }
}
