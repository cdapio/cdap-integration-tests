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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
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
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests reading from GCS (Google Cloud Storage) and writing to GCS from within a Dataproc cluster.
 */
public class GCSTest extends ETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSTest.class);
  private static final String PROFILE_NAME = "dataproc-itn-profile";
  private static final String INPUT_BLOB_NAME = "data/input/customers.csv";
  private static final String OUTPUT_BLOB_NAME = "data/output";
  private static String projectId;
  private static String serviceAccountCredentials;
  private static Storage storage;
  private List<Bucket> createdBuckets;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    // base64-encode the credentials, to avoid a commandline-parsing error, since the credentials have dashes in them
    String property = System.getProperty("google.application.credentials.base64.encoded");

    if (property != null) {
      serviceAccountCredentials = Bytes.toString(Base64.getDecoder().decode(property));
    } else {
      property = Preconditions.checkNotNull(System.getProperty("google.application.credentials.path"),
                                            "The credentials file provided is null. " +
                                              "Please make sure the path is correct and the file exists.");

      serviceAccountCredentials = Files.toString(new File(property), Charsets.UTF_8);
    }

    JsonObject serviceAccountJson = new JsonParser().parse(serviceAccountCredentials).getAsJsonObject();
    projectId = serviceAccountJson.get("project_id").getAsString();
    storage = StorageOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(serviceAccountCredentials.getBytes(StandardCharsets.UTF_8))))
      .build().getService();
  }

  @Before
  public void testSetup() throws IOException, UnauthenticatedException {
    createProfile(PROFILE_NAME);
    createdBuckets = new ArrayList<>();
  }

  @After
  public void testTearDown() {
    try {
      // Disable the profile before deleting
      URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + PROFILE_NAME + "/disable");
      getRestClient().execute(HttpRequest.post(url).build(), getClientConfig().getAccessToken());

      url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + PROFILE_NAME);
      getRestClient().execute(HttpRequest.delete(url).build(), getClientConfig().getAccessToken());
    } catch (Exception e) {
      LOG.error("Failed to delete profile.", e);
    }
    for (Bucket bucket : createdBuckets) {
      try {
        deleteBucket(bucket);
      } catch (RuntimeException e) {
        LOG.error("Unable to delete GCS bucket {}", bucket.getName(), e);
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
    createdBuckets.add(bucket);
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

  private JsonObject ofProperty(String name, String value) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", name);
    jsonObject.addProperty("value", value);
    return jsonObject;
  }

  private void createProfile(String profileName) throws IOException, UnauthenticatedException {
    Gson gson = new Gson();
    JsonArray properties = new JsonArray();
    properties.add(ofProperty("accountKey", serviceAccountCredentials));
    properties.add(ofProperty("network", "default"));
    properties.add(ofProperty("region", "global"));
    properties.add(ofProperty("zone", "us-central1-a"));
    properties.add(ofProperty("projectId", projectId));

    properties.add(ofProperty("masterNumNodes", "1"));
    properties.add(ofProperty("masterCPUs", "1"));
    properties.add(ofProperty("masterMemoryMB", "4096"));
    properties.add(ofProperty("masterDiskGB", "100"));
    properties.add(ofProperty("workerNumNodes", "2"));
    properties.add(ofProperty("workerCPUs", "1"));
    properties.add(ofProperty("workerMemoryMB", "4096"));
    properties.add(ofProperty("workerDiskGB", "100"));

    JsonObject provisioner = new JsonObject();
    provisioner.addProperty("name", "gcp-dataproc");
    provisioner.add("properties", properties);

    JsonObject jsonObj = new JsonObject();
    jsonObj.add("provisioner", provisioner);

    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + profileName);
    HttpRequest httpRequest = HttpRequest.put(url).withBody(gson.toJson(jsonObj)).build();
    getRestClient().execute(httpRequest, getAccessToken());
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
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
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
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(Collections.singletonMap("system.profile.name", PROFILE_NAME));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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

  private void assertGCSContents(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
    Assert.assertEquals(content, new String(blob.getContent(), StandardCharsets.UTF_8));
  }

  private ETLStage createCopyStage(String name, String src, String dest, boolean recursive) {
    return new ETLStage(name, new ETLPlugin("GCSCopy", Action.PLUGIN_TYPE,
                                            ImmutableMap.of("projectId", projectId,
                                                            "sourcePath", src,
                                                            "destPath", dest,
                                                            "recursive", String.valueOf(recursive))));
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
                                   new ETLPlugin("File",
                                                 BatchSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(
                                                   "schema", sourceSchema,
                                                   "format", "text",
                                                   "referenceName", "gcs-input",
                                                   "path", createPath(bucket, INPUT_BLOB_NAME))));

    ETLStage sink = new ETLStage("FileSinkStage", new ETLPlugin("File",
                                                                BatchSink.PLUGIN_TYPE,
                                                                ImmutableMap.of(
                                                                  "path", createPath(bucket, OUTPUT_BLOB_NAME),
                                                                  "format", "delimited",
                                                                  "referenceName", "gcs-output")));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app("GCSToGCS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(Collections.singletonMap("system.profile.name", PROFILE_NAME));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + source.getName() + ".records.out", 101, 10);
    checkMetric(tags, "user." + sink.getName() + ".records.in", 101, 10);

    String successFile = OUTPUT_BLOB_NAME + "/_SUCCESS";
    Assert.assertNotNull(bucket.get(successFile));

    List<Blob> outputBlobs = new ArrayList<>();
    Map<String, String> retrievedIdToRowMap = new HashMap<>();
    for (Blob blob : bucket.list().iterateAll()) {
      if (blob.getName().startsWith(OUTPUT_BLOB_NAME + "/") && !successFile.equals(blob.getName())) {
        outputBlobs.add(blob);
        byte[] content = blob.getContent();
        String contentString = Preconditions.checkNotNull(Bytes.toString(content));
        if (!contentString.isEmpty()) {
          // The way that the content is parsed, it assumes that there is only one output split with content.
          // This is because the first row of the content is skipped, because it contains the headers.
          retrievedIdToRowMap.putAll(parseIdToRow(contentString, true));
        }
      }
    }

    LOG.debug("Output blobs: {}", outputBlobs);
    // check that the output content is equivalent to the input content. Output content files are not guaranteed to be
    // split the same way, so we put the data into a map before checking equality.
    Assert.assertEquals(idToRowMap, retrievedIdToRowMap);
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
