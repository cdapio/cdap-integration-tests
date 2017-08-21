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
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryStore;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.remote.dataset.table.TableDatasetApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
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
   * Test deploy app under authorization.
   */
  @Test
  public void testDeployApp() throws Exception {

    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = testNamespace.getNamespaceId();
    ApplicationId appId = namespaceId.app(PurchaseApp.APP_NAME);

    // pre-grant all required privileges
    // admin user will be able to create namespace and retrieve the status of programs(needed for teardown)
    ImmutableMap.Builder<EntityId, Set<Action>> adminPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(namespaceId, EnumSet.of(Action.ADMIN))
      // TODO: these can be put into teardown when we migrate to wildcard privilege
      .put(appId.service("PurchaseHistoryService"), EnumSet.of(Action.ADMIN))
      .put(appId.service("UserProfileService"), EnumSet.of(Action.ADMIN))
      .put(appId.service("CatalogLookup"), EnumSet.of(Action.ADMIN))
      .put(appId.flow("PurchaseFlow"), EnumSet.of(Action.ADMIN))
      .put(appId.workflow("PurchaseHistoryWorkflow"), EnumSet.of(Action.ADMIN))
      .put(appId.mr("PurchaseHistoryBuilder"), EnumSet.of(Action.ADMIN));

    // Privileges needed to create datasets and streams
    Map<EntityId, Set<Action>> dsStreamCreationPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(namespaceId.dataset("frequentCustomers"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.stream("purchaseStream"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("userProfiles"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("history"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("purchases"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.datasetModule(PurchaseHistoryStore.class.getName()), EnumSet.of(Action.ADMIN))
      .put(namespaceId.datasetType(PurchaseHistoryStore.class.getName()), EnumSet.of(Action.ADMIN))
      .build();

    // carol will be able to create the purchase app
    ImmutableMap.Builder<EntityId, Set<Action>> appDeployPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      // TODO: remove the artifact version when we have the pr merged
      .put(namespaceId.artifact(PurchaseApp.class.getSimpleName(), "1.0.0-SNAPSHOT"), EnumSet.of(Action.ADMIN));
    String namespacePrincipal = testNamespace.getConfig().getPrincipal();
    if (namespacePrincipal != null) {
      adminPrivileges.put(new KerberosPrincipalId(namespacePrincipal), EnumSet.of(Action.ADMIN));
      appDeployPrivileges.put(new KerberosPrincipalId(namespacePrincipal),
                              EnumSet.of(Action.ADMIN));
      // if impersonation is involved, impersonated user will be responsible to create the dataset
      setUpPrivilegeAndRegisterForDeletion(namespacePrincipal, dsStreamCreationPrivileges);
    } else {
      // else the requesting user will need the privileges
      appDeployPrivileges.putAll(dsStreamCreationPrivileges);
    }
    setUpPrivilegeAndRegisterForDeletion(ADMIN_USER, adminPrivileges.build());
    setUpPrivilegeAndRegisterForDeletion(CAROL, appDeployPrivileges.build());

    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    ApplicationManager appManager =
      getTestManager(carolConfig, carolClient).deployApplication(namespaceId, PurchaseApp.class);

    // carol should not able to start the program since he does not have execute privilege on program
    try {
      appManager.startProgram(appId.flow("PurchaseFlow"));
      Assert.fail();
    } catch (Exception e) {
      // expected
      // TODO: change to Unauthorized exception
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
    NamespaceId namespaceId = testNamespace.getNamespaceId();

    userGrant(ADMIN_USER, namespaceId, Action.ADMIN);
    String principal = testNamespace.getConfig().getPrincipal();
    if (principal != null) {
      userGrant(ADMIN_USER, new KerberosPrincipalId(principal), Action.ADMIN);
    }
    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    try {
      getTestManager(carolConfig, carolClient).deployApplication(namespaceId, PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      // expected
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  /**
   * Test dataset read/write in a program. Two apps are deployed in different namespaces owned by user1 and user2
   * user1 will have write on its dataset, and user2 will have read on user1's dataset,
   * test user1 is able to write to the dataset and user2 is able to read when program is running.
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

    NamespaceId testNs1 = namespaceMeta1.getNamespaceId();
    NamespaceId testNs2 = namespaceMeta2.getNamespaceId();
    ApplicationId appId1 = testNs1.app(TableDatasetApp.class.getSimpleName());
    ApplicationId appId2 = testNs2.app(TableDatasetApp.class.getSimpleName());

    // todo: remove this once we support endpoint enforcement
    String user1 = appOwner1 == null ? namespaceMeta1.getConfig().getPrincipal() : appOwner1;
    String user2 = appOwner2 == null ? namespaceMeta2.getConfig().getPrincipal() : appOwner2;
    if (user1 == null || user2 == null) {
      Assert.fail("This test can only be used when impersonation is enabled");
    }

    // pre-grant all required privileges
    // admin user will be able to create namespace and retrieve the status of programs(needed for teardown)
    Map<EntityId, Set<Action>> adminPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      // privilege to create namespace
      .put(testNs1, EnumSet.of(Action.ADMIN))
      .put(testNs2, EnumSet.of(Action.ADMIN))
      // privilege to run the program, admin is needed to retrieve the service url
      // TODO: remove the admin privilege when we migrate to wildcard privilege
      .put(appId1.service("DatasetService"), EnumSet.of(Action.EXECUTE, Action.ADMIN))
      .put(appId2.service("DatasetService"), EnumSet.of(Action.EXECUTE, Action.ADMIN))
      // privilege to deploy app1
      .put(appId1, EnumSet.of(Action.ADMIN))
      // TODO: remove the artifact version when we have the pr merged
      .put(testNs1.artifact(TableDatasetApp.class.getSimpleName(), "1.0.0"), EnumSet.of(Action.ADMIN))
      .put(new KerberosPrincipalId(user1), EnumSet.of(Action.ADMIN))
      // privilege to deploy app2
      .put(appId2, EnumSet.of(Action.ADMIN))
      // TODO: remove the artifact version when we have the pr merged
      .put(testNs2.artifact(TableDatasetApp.class.getSimpleName(), "1.0.0"), EnumSet.of(Action.ADMIN))
      .put(new KerberosPrincipalId(user2), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ADMIN_USER, adminPrivileges);
    // since impersonation is involved, grant privileges to create the dataset and also let user1 be able to
    // write the dataset
    DatasetId dataset1 = testNs1.dataset(datasetName);
    DatasetId dataset2 = testNs2.dataset(datasetName);
    userGrant(user1, dataset1, Action.ADMIN);
    userGrant(user1, dataset1, Action.WRITE);
    cleanUpEntities.add(dataset1);
    // grant user2 the read access to dataset in ns1
    userGrant(user2, dataset2, Action.ADMIN);
    userGrant(user2, dataset1, Action.READ);
    cleanUpEntities.add(dataset2);

    createAndRegisterNamespace(namespaceMeta1, adminConfig, adminClient);
    createAndRegisterNamespace(namespaceMeta2, adminConfig, adminClient);

    // initialize clients and configs for users user1 and user2
    ClientConfig user1Config = getClientConfig(fetchAccessToken(user1, user1 + PASSWORD_SUFFIX));
    RESTClient user1Client = new RESTClient(user1Config);

    ClientConfig user2Config = getClientConfig(fetchAccessToken(user2, user2 + PASSWORD_SUFFIX));
    RESTClient user2Client = new RESTClient(user2Config);

    user1Client.addListener(createRestClientListener());
    user2Client.addListener(createRestClientListener());

    ServiceManager user1ServiceManager =
      setupAppStartAndGetService(testNs1, adminConfig, adminClient, datasetName, appOwner1);

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
      setupAppStartAndGetService(testNs2, adminConfig, adminClient, datasetName, appOwner2);

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
