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
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.concurrent.TimeUnit;

/**
 * Tests reading from GCS (Google Cloud Storage) and writing to GCS from within a Dataproc cluster.
 */
public class GCSTest extends ETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSTest.class);
  private static final String PROFILE_NAME = "dataproc-itn-profile";
  private static final String INPUT_BLOB_NAME = "data/input/customers.csv";
  private static final String OUTPUT_BLOB_NAME = "data/output";
  private Bucket bucket;
  private Map<String, String> idToRowMap;

  @Before
  public void testSetup() throws IOException, UnauthenticatedException {
    // base64-encode the credentials, to avoid a commandline-parsing error, since the credentials have dashes in them
    String property = System.getProperty("google.application.credentials.base64.encoded");

    String serviceAccountCredentials;
    if (property != null) {
      serviceAccountCredentials = Bytes.toString(Base64.getDecoder().decode(property));
    } else {
      property = Preconditions.checkNotNull(System.getProperty("google.application.credentials.path"),
                                         "The credentials file provided is null. " +
                                                 "Please make sure the path is correct and the file exists.");

      serviceAccountCredentials = Files.toString(new File(property), Charsets.UTF_8);
    }

    JsonObject serviceAccountJson = new JsonParser().parse(serviceAccountCredentials).getAsJsonObject();
    String projectId = serviceAccountJson.get("project_id").getAsString();

    bucket = createTestBucket(projectId, serviceAccountCredentials);
    createProfile(PROFILE_NAME, projectId, serviceAccountCredentials);

    try (InputStream is = new FileInputStream("src/test/resources/customers.csv")) {
      bucket.create(INPUT_BLOB_NAME, is);
    }

    try (InputStream is = new FileInputStream("src/test/resources/customers.csv")) {
      String result = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
      idToRowMap = parseIdToRow(result, false);
    }
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

    if (bucket != null) {
      deleteBucket(bucket);
    }
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

  private void createProfile(String profileName,
                             String projectId,
                             String serviceAccountCredentials) throws IOException, UnauthenticatedException {
    Gson gson = new Gson();
    JsonArray properties = new JsonArray();
    properties.add(ofProperty("accountKey", serviceAccountCredentials));
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

  private Bucket createTestBucket(String projectId, String serviceAccountCredentials) throws IOException {
    StorageOptions storageOptions = StorageOptions
      .newBuilder()
      .setProjectId(projectId)
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(serviceAccountCredentials.getBytes(StandardCharsets.UTF_8))))
      .build();
    Storage storage = storageOptions.getService();

    String bucketPrefix = "co-cask-test-bucket-";

    Page<Bucket> bucketPages = storage.list(Storage.BucketListOption.prefix(bucketPrefix));
    List<Bucket> buckets = Lists.newArrayList(bucketPages.iterateAll());
    LOG.info("Existing bucket list: {}", buckets);

    String bucketName = bucketPrefix + System.currentTimeMillis();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));
    LOG.info("Bucket {} created.", bucket);
    return bucket;
  }

  private String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  @Test
  public void testGCSToGCS() throws Exception {
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
