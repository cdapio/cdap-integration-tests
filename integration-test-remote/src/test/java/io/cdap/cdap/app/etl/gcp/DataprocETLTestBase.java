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
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  private static int workerCPUs;
  private static int workerMemMB;
  private static int workerCount;
  protected static final ArtifactSelectorConfig GOOGLE_CLOUD_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-cloud", "[0.0.0, 100.0.0)");

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
    workerCPUs = Integer.parseInt(System.getProperty("google.dataproc.worker.cpu", "4"));
    workerMemMB = 1024 * Integer.parseInt(System.getProperty("google.dataproc.worker.mem.gb", "15"));
    workerCount = Integer.parseInt(System.getProperty("google.dataproc.worker.count", "3"));
  }

  @Before
  public void testSetup() throws Exception {
    createProfile(getProfileName());
    innerSetup();
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
  }

  protected String getProfileName() {
    return PROFILE_NAME;
  }

  protected void startWorkFlow(ApplicationManager appManager, ProgramRunStatus expectedStatus) throws Exception {
    startWorkFlow(appManager, expectedStatus, Collections.emptyMap());
  }

  protected void startWorkFlow(ApplicationManager appManager, ProgramRunStatus expectedStatus,
                               Map<String, String> args) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> fullArgs = new HashMap<>();
    fullArgs.put("system.profile.name", getProfileName());
    fullArgs.putAll(args);
    startAndWaitForRun(workflowManager, expectedStatus, fullArgs, 15, TimeUnit.MINUTES);
  }

  protected static String getServiceAccountCredentials() {
    return serviceAccountCredentials;
  }

  protected static String getProjectId() {
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
    properties.add(ofProperty("workerNumNodes", String.valueOf(workerCount)));
    properties.add(ofProperty("workerCPUs", String.valueOf(workerCPUs)));
    properties.add(ofProperty("workerMemoryMB", String.valueOf(workerMemMB)));
    properties.add(ofProperty("workerDiskGB", "100"));
    properties.add(ofProperty("preferExternalIP", "true"));

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
