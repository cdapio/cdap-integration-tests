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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineApp;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.sportresults.ScoreCounter;
import co.cask.cdap.sportresults.SportResults;
import co.cask.cdap.sportresults.UploadService;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Impersonation Tests
 */
public class ImpersonationTest extends AudiTestBase {
  private static final NamespaceId NS1 = new NamespaceId("ns1");
  private static final NamespaceId NS2 = new NamespaceId("ns2");
  private static final String ALICE = "alice";
  private static final String EVE = "eve";
  private static final String BOB = "bob";

  private static final String FANTASY_2014 =
    "2014/1/3,My Team,Your Team,24,17\n" +
      "2014/3/5,My Team,Other Team,17,16\n";
  private static final String FANTASY_2015 =
    "2015/3/10,Your Team,My Team,32,12\n" +
      "2014/8/12,Other Team,Your Team,24,14\n";
  private static final String CRITTERS_2014 =
    "2015/3/10,Red Falcons,Blue Bonnets,28,17\n" +
      "2014/8/12,Green Berets,Red Falcons,23,8\n";

  @Test
  public void testBasicNamespaceImpersonation() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    try {
      namespaceClient.get(NS1);
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (NamespaceNotFoundException expected) {
      // expected
    }

    registerForDeletion(NS1);

    List<NamespaceMeta> list = namespaceClient.list();
    int initialNamespaceCount = list.size();

    NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
      .setName(NS1)
      .setPrincipal(ALICE)
      .setGroupName("admin")
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns1Meta);

    // list should contain the default namespace as well as the one explicitly created
    list = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount + 1, list.size());
    Assert.assertTrue(list.contains(ns1Meta));
    NamespaceMeta retrievedNs1Meta = namespaceClient.get(NS1);
    Assert.assertNotNull(String.format("Failed to find namespace with name %s in list: %s",
                                       NS1, Joiner.on(", ").join(list)), retrievedNs1Meta);
    Assert.assertEquals(ns1Meta, retrievedNs1Meta);
    Assert.assertEquals(ns1Meta, namespaceClient.get(NS1));

    // Test Stream creation with impersonated namespace
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    StreamId STREAM_ID = NS1.stream("streamTest");

    // create properties with user not in the same group as the user who created the namespace
    StreamProperties streamProperties =
      new StreamProperties(0L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                       ImmutableMap.<String, String>of()), 128, null, EVE);
    try {
      streamClient.create(STREAM_ID, streamProperties);
      Assert.fail("Expected stream creation to fail for this user");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Failed to create directory at")));
      try {
        streamClient.getConfig(STREAM_ID);
      } catch (Exception ioe) {
        Assert.assertTrue(ioe.getMessage().contains(String.format("was not found")));
      }
    }

    // check if stream can be created in the namespace by the owner user
    streamProperties = new StreamProperties(1L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                               ImmutableMap.<String, String>of()), 128, null, ALICE);

    streamClient.create(STREAM_ID, streamProperties);
    Assert.assertEquals(streamProperties, streamClient.getConfig(STREAM_ID));

    // check if user in the same group as owner can also create a stream
    StreamId STREAM_ID2 = NS1.stream("streamTest2");
    streamProperties = new StreamProperties(1L,
                                            new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                                    ImmutableMap.<String, String>of()), 128, null, BOB);
    streamClient.create(STREAM_ID2, streamProperties);
    Assert.assertEquals(streamProperties, streamClient.getConfig(STREAM_ID2));


    // Test Datasets
    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());
    DatasetId testDatasetInstance = NS1.dataset("testDataset");

    try {
      datasetClient.create(testDatasetInstance, new DatasetInstanceConfiguration("table", null, null, EVE));
      Assert.fail("Expected dataset creation to fail for this user");
    } catch (Exception expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Insufficient permissions")));
      try {
        datasetClient.get(testDatasetInstance);
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getMessage().contains(String.format("was not found")));
      }
    }

    datasetClient.create(testDatasetInstance, new DatasetInstanceConfiguration("table", null, null, ALICE));

    // Verify owner was able to create the dataset
    Assert.assertTrue(datasetClient.exists(testDatasetInstance));

    // Test Apps
    ArtifactId artifactId = NS1.artifact("WikipediaPipelineArtifact", "1.0.0");
    getTestManager().addAppArtifact(artifactId, WikipediaPipelineApp.class);

    ArtifactSummary artifactSummary =  new ArtifactSummary("WikipediaPipelineArtifact", "1.0.0");
    ApplicationId applicationId = NS1.app(WikipediaPipelineApp.class.getSimpleName());

    // deploy should fail when trying with user not in the same group as the namespace config
    try {
      deployApplication(applicationId, new AppRequest(artifactSummary, null, EVE));
      Assert.fail("Expected deploy app to fail for this user");
    } catch (Exception expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Insufficient permissions")));
    }

    // Check if application can be deployed by onwer
    deployApplication(applicationId, new AppRequest(artifactSummary, null, ALICE));

    // after deleting the explicitly created namespaces, only default namespace should remain in namespace list
    namespaceClient.delete(NS1);

    list = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount, list.size());
  }

  @Test
  public void testAcrossNamespaces() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();

    registerForDeletion(NS1);
    NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
      .setName(NS1)
      .setDescription("testNamespace1")
      .setPrincipal(ALICE)
      .setGroupName("deployer")
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns1Meta);

    ArtifactId artifactId = NS1.artifact("SportResults", "1.0.0");
    getTestManager().addAppArtifact(artifactId, SportResults.class);

    ArtifactSummary artifactSummary =  new ArtifactSummary("SportResults", "1.0.0");
    ApplicationId applicationId = NS1.app(SportResults.class.getSimpleName());

    deployApplication(applicationId, new AppRequest(artifactSummary, null, BOB));

    registerForDeletion(NS2);
    NamespaceMeta ns2Meta = new NamespaceMeta.Builder()
      .setName(NS2)
      .setDescription("testNamespace2")
      .setPrincipal(EVE)
      .setGroupName("deployer")
      .setKeytabURI(getKeytabURIforPrincipal(EVE))
      .build();
    namespaceClient.create(ns2Meta);

    ArtifactId artifactId2 = NS2.artifact("SportResults", "1.0.0");
    getTestManager().addAppArtifact(artifactId2, SportResults.class);
    ApplicationId applicationId2 = NS2.app(SportResults.class.getSimpleName());

    ApplicationManager applicationManager = deployApplication(applicationId2,
                                                              new AppRequest(artifactSummary, null, BOB));

    Map<String, String> args = new HashMap<>();
    // Have this program read and write from the Dataset in different namespace
    args.put("namespace", NS1.getNamespace());
    ServiceManager serviceManager =
      applicationManager.getServiceManager(UploadService.class.getSimpleName()).start(args);
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    serviceManager.waitForStatus(true);

    // upload a few dummy results
    URL url = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS*2, TimeUnit.SECONDS);
    uploadResults(url, "fantasy", 2014, FANTASY_2014);
    uploadResults(url, "fantasy", 2015, FANTASY_2015);
    uploadResults(url, "critters", 2014, CRITTERS_2014);

    args = new HashMap<>();
    args.put("league", "fantasy");
    // Have this program read and write from the Dataset in different namespace
    args.put("namespace", NS1.getNamespace());
    applicationManager.getMapReduceManager(ScoreCounter.class.getSimpleName()).start(args);
  }

  // write a file to the file set using the service
  private void uploadResults(URL url, String league, int season, String content) throws Exception {
    URL fullURL = new URL(url, String.format("leagues/%s/seasons/%d", league, season));
    HttpResponse response = getRestClient().execute(HttpMethod.PUT, fullURL, content,
                                                      ImmutableMap.<String, String>of("Content-type", "text/csv"),
                                                      getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  private String getKeytabURIforPrincipal(String principal) throws Exception {
    ConfigEntry configEntry = getMetaClient().getCDAPConfig().get(Constants.Security.KEYTAB_PATH);
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s", Constants.Security.KEYTAB_PATH);
    String name = new KerberosName(principal).getShortName();
    return configEntry.getValue().replace(Constants.USER_NAME_SPECIFIER, name);
  }

  private void createTestData(NamespaceId namespaceId) throws Exception {
    StreamId likesStream = namespaceId.stream("pageTitleStream");
    StreamManager likesStreamManager = getTestManager().getStreamManager(likesStream);
    String like1 = "{\"name\":\"Metallica\",\"id\":\"107926539230502\",\"created_time\":\"2015-06-25T17:14:47+0000\"}";
    String like2 = "{\"name\":\"grunge\",\"id\":\"911679552186992\",\"created_time\":\"2015-07-20T17:37:04+0000\"}";
    likesStreamManager.send(like1);
    likesStreamManager.send(like2);
  }
}
