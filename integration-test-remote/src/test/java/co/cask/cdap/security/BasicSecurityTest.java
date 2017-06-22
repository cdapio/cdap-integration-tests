/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.remote.dataset.table.TableDatasetApp;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/**
 * Integration tests for Authorization. The users here need to be same as in auth.json. The password for the users
 * is their user name suffixed by the word "password".
 *
 * We create a namespace for most of the test cases since we want to make sure the privilege for each user is clean.
 */
public class BasicSecurityTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BasicSecurityTest.class);
  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();
  private static final String VERSION = "1.0.0";
  private static final String ADMIN_USER = "cdapitn";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";
  private static final String CAROL = "carol";
  private static final String EVE = "eve";
  private static final String PASSWORD_SUFFIX = "password";
  private static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";
  private static final NamespaceId TEST_NAMESPACE = new NamespaceId("authorization");

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  @Test
  public void testDefaultNamespaceAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    ApplicationClient applicationClient = new ApplicationClient(adminConfig, adminClient);
    applicationClient.list(NamespaceId.DEFAULT);
  }

  @Test
  public void testDefaultNamespaceAccessUnauthorized() throws Exception {
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
    try {
      applicationClient.list(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  @Test
  public void testGrantAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(carolConfig, carolClient);
    try {
      applicationClient.list(TEST_NAMESPACE);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
    // Now authorize the user to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(TEST_NAMESPACE, new Principal(CAROL, USER), Collections.singleton(Action.READ));
    applicationClient.list(TEST_NAMESPACE);
  }

  @Test
  public void testDeployApp() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal bobPrincipal = new Principal(BOB, USER);
    authorizationClient.grant(TEST_NAMESPACE, bobPrincipal, Collections.singleton(Action.WRITE));

    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());

    getTestManager(bobConfig, bobClient).deployApplication(TEST_NAMESPACE, PurchaseApp.class);

    // List the privileges for bob and bob should have all privileges for the app he deployed.
    AuthorizationClient bobAuthorizationClient = new AuthorizationClient(bobConfig, bobClient);
    Set<Privilege> privileges = bobAuthorizationClient.listPrivileges(bobPrincipal);
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

    // Bob should have 4 privileges for each entity other than TEST_NAMESPACE
    for (Map.Entry<EntityId, Integer> entry : privilegeCount.entrySet()) {
      if (!entry.getKey().equals(TEST_NAMESPACE)) {
        Assert.assertEquals(4, (int) entry.getValue());
      } else {
        Assert.assertEquals(1, (int) entry.getValue());
      }
    }
  }

  @Test
  public void testDeployAppUnauthorized() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    try {
      getTestManager(aliceConfig, aliceClient).deployApplication(TEST_NAMESPACE, PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    // Verify that the user has all the privileges on the created namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal adminPrincipal = new Principal(ADMIN_USER, USER);
    Set<Privilege> listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    int count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(TEST_NAMESPACE.getEntityName())) {
        count++;
      }
    }
    Assert.assertEquals(4, count);

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(TEST_NAMESPACE);
    Assert.assertFalse(getNamespaceClient().exists(TEST_NAMESPACE));

    // Check if the privileges are deleted
    listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(TEST_NAMESPACE.getEntityName())) {
        count++;
      }
    }
    Assert.assertEquals(0, count);
  }

  @Test
  public void testWriteWithReadAuth() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    DatasetClient datasetAdminClient = new DatasetClient(adminConfig, adminClient);
    DatasetId testDatasetinstance = TEST_NAMESPACE.dataset("testWriteDataset");
    datasetAdminClient.create(testDatasetinstance, "table");
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(TEST_NAMESPACE, new Principal(EVE, USER),
                              Collections.singleton(Action.READ));
    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());
    DatasetClient datasetClient = new DatasetClient(eveConfig, eveClient);
    try {
      datasetClient.truncate(testDatasetinstance);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // Expected
    }
  }

  @Test
  // todo : move this to impersonation test
  // Grant a user WRITE access on a dataset.
  // Try to get the dataset from a program and call a WRITE and READ method on it.
  public void testAuthorization() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String datasetName = "testReadDataset";

    NamespaceId testNs1 = new NamespaceId("auth1");
    NamespaceId testNs2 = new NamespaceId("auth2");
    List<NamespaceId> namespaceList = new ArrayList<>();
    namespaceList.add(testNs1);
    namespaceList.add(testNs2);

    createNamespaces(namespaceList, adminConfig, adminClient);
    registerForDeletion(testNs1, testNs2);
    // initialize clients and configs for users alice and eve
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);

    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);

    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);

    aliceClient.addListener(createRestClientListener());
    eveClient.addListener(createRestClientListener());

    // set-up privileges
    // alice has {admin, read, write, execute} on TEST_NS1, eve can read TEST_NS1
    // eve has {admin, read, write, execute} on TEST_NS2, alive can read TEST_NS2
    authorizationClient.grant(testNs1, new Principal(ALICE, USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs2, new Principal(ALICE, USER), Collections.singleton(Action.READ));

    authorizationClient.grant(testNs2, new Principal(EVE, USER),
                              ImmutableSet.of(Action.WRITE, Action.READ, Action.EXECUTE, Action.ADMIN));
    authorizationClient.grant(testNs1, new Principal(EVE, USER), Collections.singleton(Action.EXECUTE));

    ServiceManager aliceServiceManager =
      setupAppStartAndGetService(testNs1, aliceConfig, aliceClient, datasetName, ALICE);

    // grant privilege on dataset to EVE after its created
    authorizationClient.grant(new DatasetId(testNs1.getNamespace(), datasetName),
                              new Principal(EVE, USER), Collections.singleton(Action.READ));

    try {
      // alice user writes an entry to the dataset
      URL serviceURL = aliceServiceManager.getServiceURL();
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col"), Bytes.toBytes("value"));
      HttpResponse httpResponse = aliceClient.execute(HttpMethod.POST,
                                                      serviceURL.toURI().resolve("put").toURL(),
                                                      GSON.toJson(put), new HashMap<String, String>(),
                                                      aliceConfig.getAccessToken());
      Assert.assertEquals(200, httpResponse.getResponseCode());
    } finally {
      aliceServiceManager.stop();
      aliceServiceManager.waitForRun(ProgramRunStatus.KILLED,
                                     PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    ServiceManager eveServiceManager =
      setupAppStartAndGetService(testNs2, eveConfig, eveClient, datasetName, EVE);

    try {
      // try to get the entry written by alice user for the dataset owned by alice
      // eve has read access on it, so read should succeed
      URL serviceURL = eveServiceManager.getServiceURL();
      Get get = new Get(Bytes.toBytes("row"), Bytes.toBytes("col"));
      String path = String.format("namespaces/%s/datasets/%s/get", testNs1.getNamespace(), datasetName);
      HttpResponse httpResponse = eveClient.execute(HttpMethod.POST,
                                                    serviceURL.toURI().resolve(path).toURL(),
                                                    GSON.toJson(get), new HashMap<String, String>(),
                                                    eveConfig.getAccessToken());
      Assert.assertEquals(200, httpResponse.getResponseCode());

      // try a put, it should fail, as eve doesn't have permission to write
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col2"), Bytes.toBytes("val2"));
      String putPath = String.format("namespaces/%s/datasets/%s/put", testNs1.getNamespace(), datasetName);
      try {
        eveClient.execute(HttpMethod.POST,
                          serviceURL.toURI().resolve(putPath).toURL(),
                          GSON.toJson(put), new HashMap<String, String>(),
                          eveConfig.getAccessToken());
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
      }
    } finally {
      eveServiceManager.stop();
      eveServiceManager.waitForRun(ProgramRunStatus.KILLED,
                                   PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  // This test will only work with list namespaces currently, since to list other entities, we need privileges
  // on the corresponding namespace, and that will make the user be able to list any entity in the namespace.
  @Test
  public void testListEntities() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    createAndRegisterNamespace(TEST_NAMESPACE, adminConfig, adminClient);

    // Now authorize user bob to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(TEST_NAMESPACE, new Principal(BOB, USER), Collections.singleton(Action.WRITE));

    ClientConfig bobConig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConig);
    bobClient.addListener(createRestClientListener());
    NamespaceClient bobNamepsaceClient = new NamespaceClient(bobConig, bobClient);

    // Bob should only be able to see TEST_NAMESPACE
    List<NamespaceMeta> namespaces = bobNamepsaceClient.list();
    Assert.assertEquals(1, namespaces.size());
    Assert.assertEquals(TEST_NAMESPACE.getEntityName(), namespaces.iterator().next().getName());

    // Create a stream with bob
    StreamId streamId = TEST_NAMESPACE.stream("testStream");
    StreamClient bobStreamClient = new StreamClient(bobConig, bobClient);
    bobStreamClient.create(streamId);

    // cdapitn should be able to list the stream since he has privileges on the namespace
    StreamClient adminStreamClient = new StreamClient(adminConfig, adminClient);
    List<StreamDetail> streams = adminStreamClient.list(TEST_NAMESPACE);
    Assert.assertEquals(1, namespaces.size());
    Assert.assertEquals(streamId.getEntityName(), streams.iterator().next().getName());

    AuthorizationClient bobAuthorizationClient = new AuthorizationClient(bobConig, bobClient);
    // simply grant READ on eve will not let Eve list the stream since Eve does not have privilege on the namespace
    bobAuthorizationClient.grant(streamId, new Principal(EVE, USER), Collections.singleton(Action.READ));

    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());

    StreamClient eveStreamClient = new StreamClient(eveConfig, eveClient);
    try {
      // Eve should not be able to list the streams since Eve does not have privilege on the namespace
      eveStreamClient.list(TEST_NAMESPACE);
      Assert.fail();
    } catch (Exception ex) {
      // expected
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  private ServiceManager setupAppStartAndGetService(NamespaceId namespaceId, ClientConfig clientConfig,
                                                    RESTClient restClient, String datasetName,
                                                    String ownerPrincipal) throws Exception {
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

  private void createNamespaces(List<NamespaceId> namespaceIdList,
                                ClientConfig clientConfig, RESTClient restClient) throws Exception {
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig, restClient);

    for (NamespaceId namespaceId : namespaceIdList) {
      NamespaceMeta nsMeta1 = new NamespaceMeta.Builder()
        .setName(namespaceId.getNamespaceId())
        .setDescription("Namespace for authorization test")
        .build();
      namespaceClient.create(nsMeta1);
    }
  }

  private void createAndRegisterNamespace(NamespaceId namespaceId, ClientConfig config,
                                          RESTClient client) throws Exception {
    NamespaceMeta meta = new NamespaceMeta.Builder()
      .setName(namespaceId.getEntityName())
      .setDescription("Namespace for authorization test")
      .build();
    new NamespaceClient(config, client).create(meta);
    registerForDeletion(namespaceId);
  }
}
