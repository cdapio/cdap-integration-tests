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

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare.Projects.Locations.Datasets;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare.Projects.Locations.Datasets.FhirStores;
import com.google.api.services.healthcare.v1beta1.CloudHealthcareScopes;
import com.google.api.services.healthcare.v1beta1.model.Dataset;
import com.google.api.services.healthcare.v1beta1.model.FhirStore;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.Tasks;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants.Reference;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests reading from FHIR store and writing to FHIR store from within a Dataproc cluster.
 */
public class FhirBatchConnectorsTest extends DataprocETLTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FhirBatchConnectorsTest.class);
  private static final String DATASET_URL_PATTERN =
      "projects/%s/locations/%s/datasets/%s";
  private static final String FHIRSTORE_URL_PATTERN =
      "projects/%s/locations/%s/datasets/%s/fhirStores/%s";
  private static final String LOCATION_URL_PATTERN =
      "projects/%s/locations/%s";
  private static final ArtifactSelectorConfig CLOUD_HEALTHCARE_ARTIFACT =
      new ArtifactSelectorConfig("SYSTEM", "connectors-batch", "[0.0.0, 100.0.0)");
  private static final String CREDENTIALS_FIELD = "google.application.credentials.path";

  private static final String TEST_DATASET_NAME = "test-dataset";
  private static final String TEST_SOURCE_STORE = "test-source-store";
  private static final String TEST_DESTINATION_STORE = "test-destination-store";

  private CloudHealthcare client;
  private GoogleCredentials credentials;
  private HttpClient httpClient = HttpClients.createDefault();
  private JsonParser jsonParser = new JsonParser();

  @Override
  protected void innerSetup() throws Exception {
    LOG.info(String.format("Setting up test using namespace %s.", TEST_NAMESPACE.toString()));
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId datapipelineId =
            NamespaceId.DEFAULT.artifact("connectors-batch","1.0.0-SNAPSHOT");
        if (!pluginExists(datapipelineId, BatchSource.PLUGIN_TYPE, "FhirStoreSource")
            || !pluginExists(datapipelineId, BatchSink.PLUGIN_TYPE, "FhirStoreSink")) {
          return false;
        }
      } catch (ArtifactNotFoundException e) {
        // happens if the relevant artifact(s) were not added yet
        return false;
      }
      return true;
    }, 1, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);

    LOG.info("Retrieving Google Credentials.");
    String credentialsPath = System.getProperty(CREDENTIALS_FIELD);
    this.credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath)).createScoped(
        Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    this.credentials.refresh();
    this.client = new CloudHealthcare.Builder(new NetHttpTransport(), new GsonFactory(), new HttpCredentialsAdapter(credentials)).build();
    LOG.info("Start setting up FHIR Stores for test.");
    setupFhirStores();
    LOG.info("Finished setting up FHIR Stores for test.");
  }

  private void setupFhirStores() throws Exception {
    String project = getProjectId();
    String locationUrl = String.format(LOCATION_URL_PATTERN, project, DEFAULT_REGION);
    Dataset dataset = new Dataset();

    Datasets.Create request = client.projects().locations().datasets().create(locationUrl, dataset);
    request.setDatasetId(TEST_DATASET_NAME);
    try {
      Operation operation = request.execute();
      System.out.println(operation.toPrettyString());
      while (operation.getDone() == null || !operation.getDone()) {
        // Update the status of the operation with another request.
        Thread.sleep(500); // Pause for 500ms between requests.
        operation =
            client
                .projects()
                .locations()
                .datasets()
                .operations()
                .get(operation.getName())
                .execute();
      }
      System.out.println("Dataset created. Response content: " + operation.getResponse());
    } catch (Exception ex) {
      System.out.printf("Error during request execution: %s\n", ex.toString());
      ex.printStackTrace(System.out);
    }

    String datasetName = String.format(DATASET_URL_PATTERN, project, DEFAULT_REGION, TEST_DATASET_NAME);
    FhirStore content = new FhirStore();
    FhirStores.Create createSrcStoreRequest = client.projects().locations().datasets().fhirStores().create(datasetName, content).setFhirStoreId(TEST_SOURCE_STORE);

    FhirStore sourceResponse = createSrcStoreRequest.execute();
    FhirStores.Create createDestStoreRequest = client.projects().locations().datasets().fhirStores().create(datasetName, content).setFhirStoreId(TEST_DESTINATION_STORE);
    FhirStore destinationResponse = createDestStoreRequest.execute();

    LOG.info("FHIR Stores created.");
    LOG.info(String.format("Source store: %s.", sourceResponse.toPrettyString()));
    LOG.info(String.format("Destination store: %s", destinationResponse.toPrettyString()));

    LOG.info("Adding dummy resource to source FHIR store.");
    StringEntity requestEntity = new StringEntity("{\"resourceType\":\"Patient\", \"language\":\"en\"}");
    String sourceStoreName = String.format(FHIRSTORE_URL_PATTERN, project, DEFAULT_REGION, TEST_DATASET_NAME, TEST_SOURCE_STORE);
    String uri = String.format("%sv1beta1/%s/fhir/Patient", client.getRootUrl(), sourceStoreName);
    URIBuilder uriBuilder = new URIBuilder(uri).setParameter("access_token", credentials.getAccessToken().getTokenValue());
    HttpUriRequest createResourceRequest = RequestBuilder.post().setUri(uriBuilder.build()).setEntity(requestEntity).addHeader("Content-Type", "application/fhir+json")
        .addHeader("Accept-Charset", "utf-8")
        .addHeader("Accept", "application/fhir+json; charset=utf-8")
        .build();

    HttpResponse createResourceResponse = httpClient.execute(createResourceRequest);
    if (createResourceResponse.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
      LOG.error("Failed to create new resource in source store");
      innerTearDown();
      throw new RuntimeException();
    }
    LOG.info(String.format("Dummy FHIR resource created: %s.", EntityUtils.toString(createResourceResponse.getEntity())));
  }

  private boolean pluginExists(ArtifactId dataPipelineId, String pluginType, String name) throws Exception {
    List<PluginSummary> summary = artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM);
    return summary.stream()
        .anyMatch(pluginSummary -> name.equals(pluginSummary.getName()));
  }

  @Override
  protected void innerTearDown() {
    LOG.info("Starting tear down.");
    String datasetName = String.format(DATASET_URL_PATTERN, getProjectId(), DEFAULT_REGION, TEST_DATASET_NAME);
    try {
      Datasets.Delete request = client.projects().locations().datasets().delete(datasetName);
      request.execute();
      LOG.info(String.format("Dataset %s deleted.", TEST_DATASET_NAME));
    } catch (IOException ex) {
      LOG.error("Failed to delete dataset.", ex);
    }
  }

  @Test
  public void fhirSourceToFhirSink() throws Exception {
    // Configs for FHIR Store source.
    Map<String, String> sourceProperties = new ImmutableMap.Builder()
        .put(Reference.REFERENCE_NAME, "mockFhirSource")
        .put("tag", "Batch")
        .put("project", getProjectId())
        .put("location", DEFAULT_REGION)
        .put("dataset", TEST_DATASET_NAME)
        .put("fhirStore", TEST_SOURCE_STORE)
        .build();

    // Configs for FHIR Store sink.
    Map<String, String> sinkProperties = new ImmutableMap.Builder()
        .put(Reference.REFERENCE_NAME, "mockFhirSink")
        .put("tag", "Batch")
        .put("project", getProjectId())
        .put("location", DEFAULT_REGION)
        .put("dataset", TEST_DATASET_NAME)
        .put("fhirStore", TEST_DESTINATION_STORE)
        .put("inputField", "body")
        .put("contentStructure", "RESOURCE")
        .build();

    // FHIR Store Source -> FHIR Store Sink in batch mode.
    ETLStage source =
        new ETLStage(
            "FhirStoreSource",
            new ETLPlugin("FhirStoreSource", BatchSource.PLUGIN_TYPE, sourceProperties, CLOUD_HEALTHCARE_ARTIFACT));
    ETLStage sink =
        new ETLStage(
            "FhirStoreSink",
            new ETLPlugin("FhirStoreSink", BatchSink.PLUGIN_TYPE, sinkProperties, CLOUD_HEALTHCARE_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
        .addStage(source)
        .addStage(sink)
        .addConnection(source.getName(), sink.getName())
        .setTimeSchedule("* * * * *")
        .build();
    ApplicationId appId = NamespaceId.DEFAULT.app("FhirBatchConnector-Test");
    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // Start the pipeline and wait for it to finish
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(Collections.singletonMap("system.profile.name", getProfileName()),
        ProgramRunStatus.COMPLETED, 30, TimeUnit.MINUTES);

    // Check FHIR Store contents and record count.
    LOG.info("Pipeline completed, verifying results.");
    verifyDestinationStoreContent();
  }

  private void verifyDestinationStoreContent() throws Exception {
    String destinationStoreName = String.format(FHIRSTORE_URL_PATTERN, getProjectId(), DEFAULT_REGION, TEST_DATASET_NAME, TEST_DESTINATION_STORE);
    String uri = String.format("%sv1beta1/%s/fhir/Patient", client.getRootUrl(), destinationStoreName);

    URIBuilder uriBuilder = new URIBuilder(uri)
        .setParameter("access_token", this.credentials.getAccessToken().getTokenValue());
    StringEntity requestEntity = new StringEntity("");

    HttpUriRequest request = RequestBuilder
        .get()
        .setUri(uriBuilder.build())
        .addHeader("Content-Type", "application/fhir+json")
        .addHeader("Accept-Charset", "utf-8")
        .addHeader("Accept", "application/fhir+json; charset=utf-8")
        .build();

    // Execute the request and process the results.
    HttpResponse response = httpClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      LOG.error(String.format(
          "Exception searching FHIR resources in destination FHIR Store: %s\n", response.getStatusLine().toString()));
      LOG.error(EntityUtils.toString(responseEntity));
      throw new RuntimeException("Test failed due to error verifying destination FHIR store.");
    }
    JsonObject jsonResponse = jsonParser.parse(EntityUtils.toString(responseEntity)).getAsJsonObject();
    Assert.assertEquals(1, jsonResponse.get("total").getAsInt());
  }

  @Test
  public void fhirSourceToFhirSink_BadFhirStoreUrl() throws Exception {
    // Configs for FHIR Store source.
    Map<String, String> sourceProperties = new ImmutableMap.Builder()
        .put(Reference.REFERENCE_NAME, "mockFhirSource")
        .put("tag", "Batch")
        .put("project", getProjectId())
        .put("location", "fake_region")
        .put("dataset", "fake_dataset")
        .put("fhirStore", TEST_SOURCE_STORE)
        .build();

    // Configs for FHIR Store sink.
    Map<String, String> sinkProperties = new ImmutableMap.Builder()
        .put(Reference.REFERENCE_NAME, "mockFhirSink")
        .put("tag", "Batch")
        .put("project", getProjectId())
        .put("location", "fake_region")
        .put("dataset", TEST_DATASET_NAME)
        .put("fhirStore", TEST_DESTINATION_STORE)
        .put("inputField", "body")
        .put("contentStructure", "RESOURCE")
        .build();

    // FHIR Store Source -> FHIR Store Sink in batch mode.
    ETLStage source =
        new ETLStage(
            "FhirStoreSource",
            new ETLPlugin("FhirStoreSource", BatchSource.PLUGIN_TYPE, sourceProperties, CLOUD_HEALTHCARE_ARTIFACT));
    ETLStage sink =
        new ETLStage(
            "FhirStoreSink",
            new ETLPlugin("FhirStoreSink", BatchSink.PLUGIN_TYPE, sinkProperties, CLOUD_HEALTHCARE_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
        .addStage(source)
        .addStage(sink)
        .addConnection(source.getName(), sink.getName())
        .setTimeSchedule("* * * * *")
        .build();
    ApplicationId appId = NamespaceId.DEFAULT.app("FhirBatchConnector-Test");
    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // start the pipeline and wait for it to finish
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(Collections.singletonMap("system.profile.name", getProfileName()),
        ProgramRunStatus.FAILED, 10, TimeUnit.MINUTES);
  }
}
