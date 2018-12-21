/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.apps.wikipedia.TestData;
import co.cask.cdap.apps.wikipedia.WikipediaPipelineTestApp;
import co.cask.cdap.apps.wikipedia.WikipediaPipelineWorkflow;
import co.cask.cdap.apps.wikipedia.WikipediaService;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test WikipediaPipelineTestApp example with Impersonation and read/write across namespaces
 * Deploy the app in a one namespace and configure the app to create datasets with a certain owner group.
 * Deploy the app in a different namespace and configure the app to create programs. Use different groups/users to
 * verify access permissions.
 * The Test assumes these group mapping for the users:
 * alice: nscreator, deployers
 * bob: nscreator, deployers
 * eve: deployers
 */
// CDAP-14328: Ignored temporarily. Will be enabled to run in spark 2 compat distro
@Ignore
public class CrossNSAppImpersonationTest extends AudiTestBase {
  private static final String ALICE = "alice";
  private static final String EVE = "eve";
  private static final String BOB = "bob";
  private static final String NO_OWNER = "NoOwner";

  // app config which only creates datasets and a single mapreduce program
  private static final WikipediaPipelineTestApp.WikipediaTestAppConfig datasetConfig =
    new WikipediaPipelineTestApp.WikipediaTestAppConfig(null, true, false);
  // app config which creates all programs and no datasets
  private static final WikipediaPipelineTestApp.WikipediaTestAppConfig programsConfig =
    new WikipediaPipelineTestApp.WikipediaTestAppConfig(null, false, true);

  private static final Gson GSON = new Gson();
  private static final String NAMESPACE_ARG = "namespace";
  private static final NamespaceId NS_DATASET = new NamespaceId("nsdataset");
  private static final NamespaceId NS_APP = new NamespaceId("nsapp");
  private static final NamespaceId NS_APP2 = new NamespaceId("nsapp2");
  private static final String GROUP_NSCREATOR = "nscreator";
  private static final String GROUP_DEPLOYERS = "deployers";
  private static int totalRuns = 0;

  @Test
  public void testAcrossNamespaces() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    // Create a namespace for datasets with Group nscreator
    registerForDeletion(NS_DATASET);
    NamespaceMeta nsDataMeta = new NamespaceMeta.Builder()
      .setName(NS_DATASET)
      .setDescription("Namespace for datasets")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_NSCREATOR)
      .setKeytabURI(SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()))
      .build();
    namespaceClient.create(nsDataMeta);

    // Deploy app as Alice in the above namespace
    ArtifactSummary artifactSummary =  new ArtifactSummary("WikipediaPipelineTestApp", "1.0.0");

    ArtifactId nsDatasetArtifact = NS_DATASET.artifact(artifactSummary.getName(), artifactSummary.getVersion());
    getTestManager().addAppArtifact(nsDatasetArtifact, WikipediaPipelineTestApp.class);
    ApplicationId nsDatasetApplicationId =
      NS_DATASET.app(WikipediaPipelineTestApp.class.getSimpleName());
    deployApplication(nsDatasetApplicationId, new AppRequest<>(artifactSummary, datasetConfig, ALICE));

    // Create a namespace for deploying the apps with group deployers
    registerForDeletion(NS_APP);
    NamespaceMeta nsMeta = new NamespaceMeta.Builder()
      .setName(NS_APP)
      .setDescription("Namespace for the app")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_DEPLOYERS)
      .setKeytabURI(SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()))
      .build();
    namespaceClient.create(nsMeta);

    // Create data in the above application
    TestData.sendTestData(NS_DATASET, getTestManager());

    // deploy app in nsapp namespace as Eve
    ArtifactId nsAppArtifact = NS_APP.artifact(artifactSummary.getName(), artifactSummary.getVersion());
    getTestManager().addAppArtifact(nsAppArtifact, WikipediaPipelineTestApp.class);
    ApplicationId nsAppApplicationId = NS_APP.app(WikipediaPipelineTestApp.class.getSimpleName());
    // eve should be able to deploy in nsapp namespace
    ApplicationManager applicationManager = deployApplication(nsAppApplicationId,
                                                              new AppRequest<>(artifactSummary, programsConfig, EVE));

    // Verify Eve cannot write/read to datasets in namespace nsdataset, workflow run should fail.
    // run failure test case for Eve
    runWorkflowTest(applicationManager, NS_DATASET, true);

    // Now deploy the app as Bob[Group nscreator] to check group permissions
    // Create another application for deploying the apps with group deployers
    nsAppArtifact = NS_APP.artifact(artifactSummary.getName() + BOB, artifactSummary.getVersion());
    getTestManager().addAppArtifact(nsAppArtifact, WikipediaPipelineTestApp.class);
    nsAppApplicationId = NS_APP.app(WikipediaPipelineTestApp.class.getSimpleName() + BOB);
    applicationManager = deployApplication(nsAppApplicationId, new AppRequest<>(artifactSummary, programsConfig, BOB));

    // Verify Bob can run the complete workflow writing to the datasets in nsdataset namespace
    runWorkflowTest(applicationManager, NS_DATASET, false);

    // User Permission Test
    // Check if Alice herself can write to the Datasets created by Alice in another namespace
    registerForDeletion(NS_APP2);
    NamespaceMeta ns3Meta = new NamespaceMeta.Builder()
      .setName(NS_APP2)
      .setDescription("Namespace for the app")
      .setPrincipal(BOB)
      .setGroupName(GROUP_DEPLOYERS)
      .setKeytabURI(SecurityTestUtils.getKeytabURIforPrincipal(BOB, getMetaClient().getCDAPConfig()))
      .build();
    namespaceClient.create(ns3Meta);

    ArtifactId appArtifact = NS_APP2.artifact(artifactSummary.getName(), artifactSummary.getVersion());
    getTestManager().addAppArtifact(appArtifact, WikipediaPipelineTestApp.class);
    ApplicationId appApplication = NS_APP2.app(WikipediaPipelineTestApp.class.getSimpleName());
    applicationManager = deployApplication(appApplication, new AppRequest<>(artifactSummary, programsConfig, ALICE));

    // Verify Alice can run the complete workflow writing to the datasets in nsdataset namespace
    runWorkflowTest(applicationManager, NS_DATASET, false);

    // Cross Namespace Impersonation Test with permission passing
    // Check if app can write to the Datasets in another namespace by inheriting permission from the namespace
    // Deploy app in this namespace without specifying the owner principal
    ArtifactId programArtifact = NS_APP.artifact(artifactSummary.getName() + NO_OWNER, artifactSummary.getVersion());
    getTestManager().addAppArtifact(programArtifact, WikipediaPipelineTestApp.class);
    ApplicationId programApp = NS_APP.app(WikipediaPipelineTestApp.class.getSimpleName() + NO_OWNER);
    applicationManager = deployApplication(programApp, new AppRequest<>(artifactSummary, programsConfig));

    // Verify complete workflow can run while writing to the datasets in nsdataset namespace
    runWorkflowTest(applicationManager, NS_DATASET, false);
  }


  private void runWorkflowTest(ApplicationManager appManager,
                               NamespaceId datasetNamespaceId, boolean failureTest) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(WikipediaPipelineWorkflow.class.getSimpleName());
    ServiceManager serviceManager = appManager.getServiceManager(WikipediaService.class.getSimpleName());

    Map<String, String> args = new HashMap<>();
    args.put("system.resources.memory", "1024");
    args.put("min.pages.threshold", String.valueOf(1));

    // Add namespace for Datasets to args for workflow to pass args to all programs
    args.put(NAMESPACE_ARG, datasetNamespaceId.getNamespace());
    workflowManager.start(args);

    if (failureTest) {
      assertWorkflowFailure(workflowManager);
    } else {
      // Count how many times workflow runs and updates the sink datasets
      totalRuns++;
      assertWorkflowSuccess(workflowManager, serviceManager, datasetNamespaceId);
    }
  }

  private void assertWorkflowFailure(WorkflowManager workflowManager) throws Exception {
    // Wait for the current run to finish
    // Dont test for status Completed when Failed is expected to avoid hitting the timeout.
    workflowManager.waitForRuns(ProgramRunStatus.FAILED, 1, 10, TimeUnit.MINUTES);
    Assert.assertEquals(workflowManager.getHistory().size(), 1);
    Assert.assertEquals(workflowManager.getHistory().get(0).getStatus(), ProgramRunStatus.FAILED);
  }

  private void assertWorkflowSuccess(WorkflowManager workflowManager, ServiceManager serviceManager,
                                     NamespaceId datasetNamespaceId) throws Exception {
    // Wait for the current run to finish
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 10, TimeUnit.MINUTES);

    // Wait for the workflow status. The timeout here is actually a sleep so, the timeout is a low value and instead
    // we retry a large number of times.
    workflowManager.waitForStatus(false, 60, 1);

    // Start the service with args
    Map<String, String> args = new HashMap<>();
    args.put("namespace", datasetNamespaceId.getNamespace());

    serviceManager.start(args);
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertServiceResults(serviceManager);
  }

  private void assertServiceResults(ServiceManager serviceManager) throws Exception {
    // Check for clustering dataset
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL topicsURL = new URL(serviceURL, "v1/functions/lda/topics");
    HttpResponse response = getRestClient().execute(HttpRequest.get(topicsURL).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    List<Integer> topics = GSON.fromJson(response.getResponseBodyAsString(),
                         new TypeToken<List<Integer>>() { }.getType());
    Assert.assertEquals(10, topics.size());
    Assert.assertTrue(topics.contains(0));
    Assert.assertTrue(topics.contains(1));

    // check for topn dataset
    URL wordsURL = new URL(serviceURL, "v1/functions/topn/words");
    response = getRestClient().execute(HttpRequest.get(wordsURL).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    List<JsonObject> words = GSON.fromJson(response.getResponseBodyAsString(),
                                           new TypeToken<List<JsonObject>>() { }.getType());
    Assert.assertEquals(10, words.size());
    JsonObject jsonObject1 = new JsonObject();
    jsonObject1.addProperty("London", 2 * totalRuns);
    JsonObject jsonObject2 = new JsonObject();
    jsonObject2.addProperty("Metallica", 2 * totalRuns);
    Assert.assertTrue(words.contains(jsonObject1));
    Assert.assertTrue(words.contains(jsonObject2));
  }
}
