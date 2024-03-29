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
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
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
  private static String serviceAccountEmail;
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
    serviceAccountEmail = serviceAccountJson.get("client_email").getAsString();

    network = System.getProperty("google.dataproc.network", "default");
    workerCPUs = Integer.parseInt(System.getProperty("google.dataproc.worker.cpu", "4"));
    workerMemMB = 1024 * Integer.parseInt(System.getProperty("google.dataproc.worker.mem.gb", "15"));
    workerCount = Integer.parseInt(System.getProperty("google.dataproc.worker.count", "3"));
  }

  @Before
  public void testSetup() throws Exception {
    createProfile(getProfileName());
    enableProfile();
    innerSetup();
  }

  private void enableProfile() throws Exception {
    // Enable the profile if it is disabled
    URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + getProfileName());
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    ProfileStatus status = new Gson().fromJson(response.getResponseBodyAsString(), Profile.class).getStatus();
    if (status == ProfileStatus.DISABLED) {
      url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + getProfileName() + "/enable");
      getRestClient().execute(HttpRequest.post(url).withBody("").build(), getClientConfig().getAccessToken());
    }
  }

  @After
  public void testTearDown() throws Exception {
    try {
      // Disable the profile before deleting
      URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "profiles/" + getProfileName() + "/disable");
      // Empty body string is needed to avoid a 411 error (content-length header not set for post request) from the
      // new testing infrastructure. The empty string sets the content length header to 0 which avoids the issue
      getRestClient().execute(HttpRequest.post(url).withBody("").build(), getClientConfig().getAccessToken());

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
    boolean shouldDumpLogs = true;
    try {
      startAndWaitForRun(workflowManager, expectedStatus, fullArgs, 30, TimeUnit.MINUTES);
      shouldDumpLogs = false;
    } finally {
      if (shouldDumpLogs) {
        dumpLogs(appManager, workflowManager);
      }
    }
  }

  private void dumpLogs(ApplicationManager applicationManager, WorkflowManager workflowManager) throws Exception {
    // Most recent record returned by ProgramLifecycleHttpHandler#programHistoryVersioned is listed first.
    RunRecord runRecord = workflowManager.getHistory().get(0);
    String runId = runRecord.getPid();
    String appName = applicationManager.getInfo().getName();
    LOG.info("Dumping logs for pipeline {} run {}", appName, runId);
    try {
      URL url = getClientConfig().resolveNamespacedURLV3(TEST_NAMESPACE, "apps/"
        + applicationManager.getInfo().getName() + "/workflows/DataPipelineWorkflow/runs/" + runId + "/logs");
      HttpResponse res = getRestClient().execute(HttpRequest.get(url).build(),
                                                 getClientConfig().getAccessToken());
      String[] resStr = res.getResponseBodyAsString().split("[\\r\\n]+");
      LOG.info(String.format("Logs for pipeline '%s'", appName));
      for (String line : resStr) {
        LOG.info(line);
      }
    } catch (Exception e) {
      LOG.info("Failed to dump logs for pipeline", e);
    }
  }

  protected static String getServiceAccountCredentials() {
    return serviceAccountCredentials;
  }

  protected static String getProjectId() {
    return projectId;
  }

  protected static String getServiceAccountEmail() {
    return serviceAccountEmail;
  }

  protected abstract void innerSetup() throws Exception;

  protected abstract void innerTearDown() throws Exception;

  private void createProfile(String profileName) throws Exception {
    Gson gson = new Gson();
    JsonArray properties = new JsonArray();
    properties.add(ofProperty("accountKey", getServiceAccountCredentials()));
    properties.add(ofProperty("serviceAccount", getServiceAccountEmail()));
    properties.add(ofProperty("network", network));
    properties.add(ofProperty("region", "us-central1"));
    properties.add(ofProperty("projectId", getProjectId()));

    properties.add(ofProperty("masterNumNodes", "1"));
    properties.add(ofProperty("masterMachineType", "e2"));
    properties.add(ofProperty("masterCPUs", "2"));
    properties.add(ofProperty("masterMemoryMB", "8192"));
    properties.add(ofProperty("masterDiskGB", "1000"));
    properties.add(ofProperty("workerNumNodes", String.valueOf(workerCount)));
    properties.add(ofProperty("workerMachineType", "e2"));
    properties.add(ofProperty("workerCPUs", String.valueOf(workerCPUs)));
    properties.add(ofProperty("workerMemoryMB", String.valueOf(workerMemMB)));
    properties.add(ofProperty("workerDiskGB", "500"));
    properties.add(ofProperty("preferExternalIP", "true"));
    properties.add(ofProperty("stackdriverLoggingEnabled", "true"));
    properties.add(ofProperty("stackdriverMonitoringEnabled", "true"));
    properties.add(ofProperty("idleTTL", "60"));

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

  protected void createConnection(String connectionName, String connectorName) throws Exception {
    Gson gson = new Gson();
    URL baseURL = getClientConfig().resolveNamespacedURLV3(NamespaceId.SYSTEM,
                                                           "apps/pipeline/services/studio/methods/");
    JsonObject properties = new JsonObject();
    properties.addProperty("project", getProjectId());
    properties.addProperty("serviceAccountType", "filePath");
    properties.addProperty("serviceFilePath", "auto-detect");

    JsonObject artifact = new JsonObject();
    artifact.addProperty("scope", "SYSTEM");
    artifact.addProperty("name", "google-cloud");

    JsonObject plugin = new JsonObject();
    plugin.addProperty("category", "Google Cloud Platform");
    plugin.addProperty("name", connectorName);
    plugin.addProperty("type", "connector");
    plugin.add("properties", properties);
    plugin.add("artifact", artifact);

    JsonObject body = new JsonObject();
    body.addProperty("name", connectionName);
    body.addProperty("description", String.format("%s connection for running integration test", connectorName));
    body.addProperty("category", "Google Cloud Platform");
    body.add("plugin", plugin);

    String testPath = String.format("v1/contexts/%s/connections/test", TEST_NAMESPACE.getNamespace());
    HttpResponse testResponse = getRestClient()
      .execute(HttpRequest.post(new URL(baseURL, testPath)).withBody(gson.toJson(body)).build(),
               getClientConfig().getAccessToken());
    Assert.assertEquals(200, testResponse.getResponseCode());
    String createPath = String.format("v1/contexts/%s/connections/%s", TEST_NAMESPACE.getNamespace(), connectionName);
    try {
      HttpResponse createResponse = getRestClient()
        .execute(HttpRequest.put(new URL(baseURL, createPath)).withBody(gson.toJson(body)).build(),
                 getClientConfig().getAccessToken());
      Assert.assertEquals(200, createResponse.getResponseCode());
    } catch (IOException e) {
      if (e.getMessage().startsWith("409")) {
        // conflict means connection already exists
        // Ignore this and move on, since all that matters is that the connection exists.
      } else {
        throw new IOException(e);
      }
    }
    LOG.info(String.format("Connection %s created successfully", connectionName));
  }

  protected void deleteConnection(String connectionName) throws Exception {
    URL baseURL = getClientConfig().resolveNamespacedURLV3(NamespaceId.SYSTEM,
                                                           "apps/pipeline/services/studio/methods/");
    String deletePath = String.format("v1/contexts/%s/connections/%s", TEST_NAMESPACE.getNamespace(), connectionName);
    try {
      HttpResponse createResponse = getRestClient()
        .execute(HttpRequest.delete(new URL(baseURL, deletePath)).build(), getClientConfig().getAccessToken());
      Assert.assertEquals(200, createResponse.getResponseCode());
    } catch (Exception e) {
      LOG.error(String.format("Failed to delete connection: %s", connectionName), e);
    }
    LOG.info(String.format("Connection %s deleted successfully", connectionName));
  }
}
