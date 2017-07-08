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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.remote.dataset.table.TableDatasetApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/**
 * Basic authorization test base for apps
 */
public class AppAuthorizationTestBase extends AuthorizationTestBase {

  protected NamespaceMeta namespaceMeta1 = getNamespaceMeta(new NamespaceId("authorization1"), null, null,
                                                            null, null, null, null);
  protected NamespaceMeta namespaceMeta2 = getNamespaceMeta(new NamespaceId("authorization2"), null, null,
                                                            null, null, null, null);
  protected String appOwner = null;
  // todo: this should be null once we support endpoint enforcement
  protected String appOwner1 = ALICE;
  protected String appOwner2 = BOB;

  /**
   * Test deploy app under authorization and user gets all privileges on the app after the deploy.
   */
  @Test
  public void testDeployApp() throws Exception {

    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal carolPrincipal = new Principal(CAROL, USER);
    authorizationClient.grant(namespaceId, carolPrincipal, Collections.singleton(Action.WRITE));

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    String appName = PurchaseApp.class.getSimpleName();
    ArtifactId artifactId = namespaceId.artifact(appName, VERSION);
    TestManager manager = getTestManager(carolConfig, carolClient);
    manager.addAppArtifact(artifactId, PurchaseApp.class);

    ArtifactSummary appSummary = new ArtifactSummary(appName, VERSION);
    getTestManager(carolConfig, carolClient).deployApplication(namespaceId.app(appName),
                                                               new AppRequest<>(appSummary, null, appOwner));

    // List the privileges for carol and carol should have all privileges for the app he deployed.
    AuthorizationClient carolAuthorizationClient = new AuthorizationClient(carolConfig, carolClient);
    Set<Privilege> privileges = carolAuthorizationClient.listPrivileges(carolPrincipal);
    Assert.assertTrue(privileges.size() > 1);

    // Count the privileges for each entity
    Map<EntityId, Integer> privilegeCount = new HashMap<>();
    for (Privilege privilege : privileges) {
      if (privilegeCount.containsKey(privilege.getEntity())) {
        privilegeCount.put(privilege.getEntity(), privilegeCount.get(privilege.getEntity()) + 1);
      } else {
        privilegeCount.put(privilege.getEntity(), 1);
      }
    }

    // Carol should have 4 privileges for each entity other than the given namespace
    for (Map.Entry<EntityId, Integer> entry : privilegeCount.entrySet()) {
      if (!entry.getKey().equals(namespaceId)) {
        Assert.assertEquals(4, (int) entry.getValue());
      } else {
        Assert.assertEquals(1, (int) entry.getValue());
      }
    }
  }

  /**
   * Test user cannot deploy app with insufficient privilege.
   */
  @Test
  public void testDeployAppUnauthorized() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    try {
      getTestManager(carolConfig, carolClient).deployApplication(namespaceId, PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  /**
   * Grant a user WRITE access on a dataset.
   * Try to get the dataset from a program and call a WRITE and READ method on it.
   *
   * Note that this test can ONLY be used when impersonation is enabled, since currently we do not have
   * endpoint enforcement
   */
  @Test
  public void testDatasetInProgram() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String datasetName = "testReadDataset";

    NamespaceId testNs1 = createAndRegisterNamespace(namespaceMeta1, adminConfig, adminClient);
    NamespaceId testNs2 = createAndRegisterNamespace(namespaceMeta2, adminConfig, adminClient);
    DatasetId datasetId = new DatasetId(testNs1.getNamespace(), datasetName);

    // initialize clients and configs for users user1 and user2
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);

    // todo: remove this once we support endpoint enforcement
    String user1 = appOwner1 == null ? namespaceMeta1.getConfig().getPrincipal() : appOwner1;
    String user2 = appOwner2 == null ? namespaceMeta2.getConfig().getPrincipal() : appOwner2;
    if (user1 == null || user2 == null) {
      Assert.fail("This test can only be used when impersonation is enabled");
    }

    ClientConfig user1Config = getClientConfig(fetchAccessToken(user1, user1 + PASSWORD_SUFFIX));
    RESTClient user1Client = new RESTClient(user1Config);

    ClientConfig user2Config = getClientConfig(fetchAccessToken(user2, user2 + PASSWORD_SUFFIX));
    RESTClient user2Client = new RESTClient(user2Config);

    user1Client.addListener(createRestClientListener());
    user2Client.addListener(createRestClientListener());

    // set-up privileges
    // user1 has {admin, read, write, execute} on TEST_NS1, user2 has read on TEST_NS1
    // user2 has {admin, read, write, execute} on TEST_NS2, user1 has execute on TEST_NS2
    authorizationClient.grant(testNs1, new Principal(user1, USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs2, new Principal(user1, USER), Collections.singleton(Action.READ));

    authorizationClient.grant(testNs2, new Principal(user2, USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs1, new Principal(user2, USER), Collections.singleton(Action.EXECUTE));

    ServiceManager user1ServiceManager =
      setupAppStartAndGetService(testNs1, user1Config, user1Client, datasetName, appOwner1);

    // grant privilege on dataset to user2 after its created
    authorizationClient.grant(datasetId, new Principal(user2, USER), Collections.singleton(Action.READ));

    try {
      // user1 writes an entry to the dataset
      URL serviceURL = user1ServiceManager.getServiceURL();
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col"), 100L);
      HttpResponse httpResponse = user1Client.execute(HttpMethod.POST,
                                                      serviceURL.toURI().resolve("put").toURL(),
                                                      GSON.toJson(put), new HashMap<String, String>(),
                                                      user1Config.getAccessToken());
      Assert.assertEquals(200, httpResponse.getResponseCode());
    } finally {
      user1ServiceManager.stop();
      user1ServiceManager.waitForRun(ProgramRunStatus.KILLED,
                                     PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    ServiceManager user2ServiceManager =
      setupAppStartAndGetService(testNs2, user2Config, user2Client, datasetName, appOwner2);

    int runs = 0;
    try {
      URL serviceURL = user2ServiceManager.getServiceURL();
      // try to get the entry written by user2 for the dataset owned by user1
      // user2 has read access on it, so read should succeed
      String getJson = GSON.toJson(new Get(Bytes.toBytes("row"), Bytes.toBytes("col")));
      String putJson = GSON.toJson(new Put(Bytes.toBytes("row"), Bytes.toBytes("col2"), Bytes.toBytes("val2")));
      String incrementJson = GSON.toJson(new Increment(Bytes.toBytes("row"), Bytes.toBytes("col"), 1));
      HttpResponse response = executeDatasetCommand(serviceURL, user2Client, user2Config,
                                                    testNs1, datasetName, getJson, "get");
      Assert.assertEquals(200, response.getResponseCode());

      try {
        // put should fail since user2 does not have write privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, putJson, "put");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }

      try {
        // incrementAndGet should fail since user2 does not have both READ and WRITE privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                              "incrementAndGet");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }

      // grant user WRITE privilege on the dataset
      authorizationClient.grant(datasetId, new Principal(user2, USER), Collections.singleton(Action.WRITE));

      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs, PROGRAM_START_STOP_TIMEOUT_SECONDS,
                                      TimeUnit.SECONDS);
      user2ServiceManager.start();
      user2ServiceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      serviceURL = user2ServiceManager.getServiceURL();

      // put should be successful this time
      response = executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, putJson, "put");
      Assert.assertEquals(200, response.getResponseCode());

      // incrementAndGet should be successful this time since user2 has both READ and WRITE privilege on the dataset
      response = executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                                       "incrementAndGet");
      Assert.assertEquals(200, response.getResponseCode());

      // revoke READ from user2 on the dataset
      authorizationClient.revoke(datasetId, new Principal(user2, USER), Collections.singleton(Action.READ));

      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs,
                                      PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      user2ServiceManager.start();
      user2ServiceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      serviceURL = user2ServiceManager.getServiceURL();

      try {
        // get should fail since user2 does not have READ privilege now
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, getJson, "get");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }

      try {
        // increment should fail since user2 does not have both READ and WRITE privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                              "incrementAndGet");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }
    } finally {
      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs,
                                      PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private ServiceManager setupAppStartAndGetService(NamespaceId namespaceId, ClientConfig clientConfig,
                                                    RESTClient restClient, String datasetName,
                                                    @Nullable String ownerPrincipal) throws Exception {
    ArtifactId tableDatasetApp = namespaceId.artifact(TableDatasetApp.class.getSimpleName(), VERSION);
    TestManager testManager = getTestManager(clientConfig, restClient);

    testManager.addAppArtifact(tableDatasetApp, TableDatasetApp.class);

    ArtifactSummary appSummary = new ArtifactSummary(TableDatasetApp.class.getSimpleName(), VERSION);
    ApplicationId applicationId = namespaceId.app(TableDatasetApp.class.getSimpleName());

    TableDatasetApp.DatasetConfig config = new AbstractDatasetApp.DatasetConfig(datasetName);

    ApplicationManager applicationManager =
      testManager.deployApplication(applicationId, new AppRequest<>(appSummary, config, ownerPrincipal));

    ServiceManager serviceManager =
      applicationManager.getServiceManager(TableDatasetApp.DatasetService.class.getSimpleName());

    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // i have noticed this take longer than 60 seconds on CM cluster
    serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

    return serviceManager;
  }

  private HttpResponse executeDatasetCommand(URL serviceURL, RESTClient client, ClientConfig config,
                                             NamespaceId namespaceId, String datasetName,
                                             String jsonString, String method) throws Exception {
    String path = String.format("namespaces/%s/datasets/%s/%s", namespaceId.getNamespace(), datasetName, method);
    return client.execute(HttpMethod.POST,
                          serviceURL.toURI().resolve(path).toURL(),
                          jsonString, new HashMap<String, String>(),
                          config.getAccessToken());
  }
}
