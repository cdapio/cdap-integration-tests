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

/**
 * Basic authorization test base for apps
 */
public class BasicAppAuthorizationTest extends AuthorizationTestBase {

  // namespace for app1 for cross namespace test. For tests without cross namespace, use testNamespace in
  // AuthorizationTestBase instead.
  protected NamespaceMeta namespaceMeta1 = getNamespaceMeta(new NamespaceId("authorization1"), null, null,
                                                            null, null, null, null);
  // namespace for app2 for cross namespace test
  protected NamespaceMeta namespaceMeta2 = getNamespaceMeta(new NamespaceId("authorization2"), null, null,
                                                            null, null, null, null);
  // owner of app1 deployed in namespace1
  // Ideally this should be null but since any app without impersonation will be run as user cdap,
  // the privileges on other user will be useless in the test, app impersonation is required.
  // To deploy without app impersonation, set this to null.
  protected String appOwner1 = ALICE;
  // owner of app2 deployed in namespace2
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
    Principal carolPrincipal = new Principal(CAROL, Principal.PrincipalType.USER);
    authorizationClient.grant(namespaceId, carolPrincipal, Collections.singleton(Action.WRITE));

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    getTestManager(carolConfig, carolClient).deployApplication(namespaceId, PurchaseApp.class);

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

    // Bob should have 4 privileges for each entity other than the given namespace
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
    authorizationClient.grant(testNs1, new Principal(user1, Principal.PrincipalType.USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs2, new Principal(user1, Principal.PrincipalType.USER),
                              Collections.singleton(Action.READ));

    authorizationClient.grant(testNs2, new Principal(user2, Principal.PrincipalType.USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs1, new Principal(user2, Principal.PrincipalType.USER),
                              Collections.singleton(Action.EXECUTE));

    ServiceManager user1ServiceManager =
      setupAppStartAndGetService(testNs1, user1Config, user1Client, datasetName, appOwner1);

    // grant privilege on dataset to user2 after its created
    authorizationClient.grant(new DatasetId(testNs1.getNamespace(), datasetName),
                              new Principal(user2, Principal.PrincipalType.USER), Collections.singleton(Action.READ));

    try {
      // user1 writes an entry to the dataset
      URL serviceURL = user1ServiceManager.getServiceURL();
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col"), Bytes.toBytes("value"));
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

    try {
      // try to get the entry written by user2 for the dataset owned by user1
      // user2 has read access on it, so read should succeed
      URL serviceURL = user2ServiceManager.getServiceURL();
      Get get = new Get(Bytes.toBytes("row"), Bytes.toBytes("col"));
      String path = String.format("namespaces/%s/datasets/%s/get", testNs1.getNamespace(), datasetName);
      HttpResponse httpResponse = user2Client.execute(HttpMethod.POST,
                                                      serviceURL.toURI().resolve(path).toURL(),
                                                      GSON.toJson(get), new HashMap<String, String>(),
                                                      user2Config.getAccessToken());
      Assert.assertEquals(200, httpResponse.getResponseCode());

      // try a put, it should fail, as user2 doesn't have permission to write
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col2"), Bytes.toBytes("val2"));
      String putPath = String.format("namespaces/%s/datasets/%s/put", testNs1.getNamespace(), datasetName);
      try {
        user2Client.execute(HttpMethod.POST,
                            serviceURL.toURI().resolve(putPath).toURL(),
                            GSON.toJson(put), new HashMap<String, String>(),
                            user2Config.getAccessToken());
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }
    } finally {
      user2ServiceManager.stop();
      user2ServiceManager.waitForRun(ProgramRunStatus.KILLED,
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
}
