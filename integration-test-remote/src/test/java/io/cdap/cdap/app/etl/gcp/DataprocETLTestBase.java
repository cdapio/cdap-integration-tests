/*
 * Copyright © 2019 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An abstract class used for running integration tests within Dataproc.
 */
public abstract class DataprocETLTestBase extends ETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocETLTestBase.class);
  private static final String PROFILE_NAME = "dataproc-itn-profile";

  private static String projectId;
  private static String serviceAccountCredentials;
  private static String network;
  protected static final ArtifactSelectorConfig GOOGLE_CLOUD_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-cloud", "[0.0.0, 100.0.0)");
  public static Storage storage;
  protected List<String> markedForDeleteBuckets;

  @BeforeClass
  public static void testDataprocClassSetup() throws IOException {
    // base64-encode the credentials, to avoid a commandline-parsing error, since the credentials have dashes in them
    String property = System.getProperty("google.application.credentials.base64.encoded");
    if (property != null) {
      serviceAccountCredentials = Bytes.toString(Base64.getDecoder().decode(property));
    } else {
      property = Preconditions.checkNotNull(System.getProperty("google.application.credentials.path"),
                                            "The credentials file provided is null. " +
                                              "Please make sure the path is correct and the file exists.");

      serviceAccountCredentials = new String(Files.readAllBytes(Paths.get(property)), StandardCharsets.UTF_8);
    }

    JsonObject serviceAccountJson = new JsonParser().parse(serviceAccountCredentials).getAsJsonObject();
    projectId = serviceAccountJson.get("project_id").getAsString();

    network = System.getProperty("google.dataproc.network", "default");

    storage = StorageOptions.newBuilder()
      .setProjectId(DataprocETLTestBase.getProjectId())
      .setCredentials(GoogleCredentials.fromStream(
        new ByteArrayInputStream(DataprocETLTestBase.getServiceAccountCredentials()
          .getBytes(StandardCharsets.UTF_8))))
      .build().getService();
  }

  @Before
  public void testSetup() throws Exception {
    createProfile(getProfileName());
    innerSetup();
    markedForDeleteBuckets = new ArrayList<>();
  }

  @After
  public void testTearDown() throws Exception {
    try {
      // Disable the profile before deleting
      URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + getProfileName() + "/disable");
      getRestClient().execute(HttpRequest.post(url).build(), getClientConfig().getAccessToken());

      url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + getProfileName());
      getRestClient().execute(HttpRequest.delete(url).build(), getClientConfig().getAccessToken());
    } catch (Exception e) {
      LOG.error("Failed to delete profile", e);
    }
    innerTearDown();
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
  protected Bucket createBucket(String name) {
    LOG.info("Creating bucket {}", name);
    Bucket bucket = storage.create(BucketInfo.of(name));
    LOG.info("Created bucket {}", name);
    markedForDeleteBuckets.add(name);
    return bucket;
  }

  protected void deleteBucket(Bucket bucket) {
    for (Blob blob : bucket.list().iterateAll()) {
      LOG.info("Deleting blob {}", blob);
      blob.delete();
    }
    LOG.info("Deleting bucket {}", bucket);
    bucket.delete(Bucket.BucketSourceOption.metagenerationMatch());
  }

  protected void markBucketNameForDelete(String bucketName) {
    markedForDeleteBuckets.add(bucketName);
  }

  protected String createPath(Bucket bucket, String blobName) {
    return String.format("gs://%s/%s", bucket.getName(), blobName);
  }

  protected void assertGCSContents(Bucket bucket, String blobName, String content) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
    Assert.assertEquals(content, new String(blob.getContent(), StandardCharsets.UTF_8));
  }

  protected void assertNotExists(Bucket bucket, String blobName) {
    Blob blob = bucket.get(blobName);
    if (blob != null) {
      Assert.assertFalse(String.format("%s in %s exists but must not", blobName, bucket.getName()), blob.exists());
    }
  }

  protected void assertExists(Bucket bucket, String blobName) {
    Blob blob = bucket.get(blobName);
    Assert.assertNotNull(String.format("%s in %s does not exist", blobName, bucket.getName()), blob);
  }

  protected String getProfileName() {
    return PROFILE_NAME;
  }

  protected void startWorkFlow(ApplicationManager appManager, ProgramRunStatus expectedStatus) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(Collections.singletonMap("system.profile.name", getProfileName()),
                                       expectedStatus, 15, TimeUnit.MINUTES);
  }

  public static String getServiceAccountCredentials() {
    return serviceAccountCredentials;
  }

  public static String getProjectId() {
    return projectId;
  }

  protected abstract void innerSetup() throws Exception;

  protected abstract void innerTearDown() throws Exception;

  private void createProfile(String profileName) throws Exception {
    Gson gson = new Gson();
    JsonArray properties = new JsonArray();
    properties.add(ofProperty("accountKey", getServiceAccountCredentials()));
    properties.add(ofProperty("network", network));
    properties.add(ofProperty("region", "us-central1"));
    properties.add(ofProperty("projectId", getProjectId()));

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

  private JsonObject ofProperty(String name, String value) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", name);
    jsonObject.addProperty("value", value);
    return jsonObject;
  }

}
