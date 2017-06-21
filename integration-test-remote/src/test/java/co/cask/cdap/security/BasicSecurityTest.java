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
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/**
 * Integration tests for Authorization. The users here need to be same as in auth.json. The password for the users
 * is their user name suffixed by the word "password".
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

  // This is to work around https://issues.cask.co/browse/CDAP-7680
  // Where we don't delete privileges when a namespace is deleted.
  private static String generateRandomName() {
    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
    return new BigInteger(130, new SecureRandom()).toString(32);
  }

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  @Test
  public void defaultNamespaceAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    ApplicationClient applicationClient = new ApplicationClient(adminConfig, adminClient);
    applicationClient.list(NamespaceId.DEFAULT);
  }

  @Test
  public void defaultNamespaceAccessUnauthorized() throws Exception {
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
    try {
      applicationClient.list(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  @Test
  public void testGrantAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(carolConfig, carolClient);
    NamespaceId namespaceId = new NamespaceId(name);
    try {
      applicationClient.list(namespaceId);
      Assert.fail();
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
    // Now authorize the user to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(namespaceId, new Principal(CAROL, USER), Collections.singleton(Action.READ));
    applicationClient.list(namespaceId);
    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  @Test
  public void testDeployApp() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(NamespaceId.DEFAULT, new Principal(BOB, USER), Collections.singleton(Action.WRITE));

    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());

    getTestManager(bobConfig, bobClient).deployApplication(NamespaceId.DEFAULT, PurchaseApp.class);
  }

  @Test
  public void testDeployAppUnauthorized() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    try {
      getTestManager(aliceConfig, aliceClient).deployApplication(new NamespaceId(name), PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    } finally {
      // Now delete the namespace and make sure that it is deleted
      getNamespaceClient().delete(new NamespaceId(name));
      Assert.assertFalse(getNamespaceClient().exists(new NamespaceId(name)));
    }
  }

  @Test
  @Ignore
  // https://issues.cask.co/browse/CDAP-7680 Prevents us from deleting privileges.
  public void testCreatedDeletedPrivileges() throws Exception {
    // Create a namespace
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    getTestManager(adminConfig, adminClient).createNamespace(meta);
    // Verify that the user has all the privileges on the created namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal adminPrincipal = new Principal(ADMIN_USER, USER);
    Set<Privilege> listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    int count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(name)) {
        count++;
      }
    }
    Assert.assertEquals(4, count);

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(new NamespaceId(name));
    Assert.assertFalse(getNamespaceClient().exists(new NamespaceId(name)));

    // Check if the privileges are deleted
    listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(name)) {
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

    DatasetClient datasetAdminClient = new DatasetClient(adminConfig, adminClient);
    DatasetId testDatasetinstance = NamespaceId.DEFAULT.dataset("testWriteDataset");
    datasetAdminClient.create(testDatasetinstance, "table");
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(NamespaceId.DEFAULT, new Principal(EVE, USER),
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
                                                      aliceConfig.getAccessToken(), new int[0]);
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
                                                    eveConfig.getAccessToken(), new int[0]);
      Assert.assertEquals(200, httpResponse.getResponseCode());

      // try a put, it should fail, as eve doesn't have permission to write
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col2"), Bytes.toBytes("val2"));
      String putPath = String.format("namespaces/%s/datasets/%s/put", testNs1.getNamespace(), datasetName);
      try {
        eveClient.execute(HttpMethod.POST,
                          serviceURL.toURI().resolve(putPath).toURL(),
                          GSON.toJson(put), new HashMap<String, String>(),
                          eveConfig.getAccessToken(), new int[0]);
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
        .setDescription("Namespace for running app from alice")
        .build();
      namespaceClient.create(nsMeta1);
    }
  }
}
