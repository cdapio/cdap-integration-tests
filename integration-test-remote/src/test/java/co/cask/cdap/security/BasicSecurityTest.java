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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.app.crossns.DatasetCrossNSAccessWithMRApp;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.TestManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/**
 * Integration tests for Authorization. The users here need to be same as in auth.json. The password for the users
 * is their user name suffixed by the word "password".
 */
public class BasicSecurityTest extends AudiTestBase {

  private static final String ADMIN_USER = "cdapitn";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";
  private static final String CAROL = "carol";
  private static final String EVE = "eve";
  private static final String PASSWORD_SUFFIX = "password";
  private static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";
  private static final String AUTH_NAMESPACE = "authNS";
//
//  // This is to work around https://issues.cask.co/browse/CDAP-7680
//  // Where we don't delete privileges when a namespace is deleted.
//  private static String generateRandomName() {
//    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
//    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
//    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
//    return new BigInteger(130, new SecureRandom()).toString(32);
//  }

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

//  @Test
//  public void defaultNamespaceAccessUnauthorized() throws Exception {
//    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
//    RESTClient aliceClient = new RESTClient(aliceConfig);
//    aliceClient.addListener(createRestClientListener());
//
//    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
//    try {
//      applicationClient.list(NamespaceId.DEFAULT);
//      Assert.fail();
//    } catch (IOException ex) {
//      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
//    }
//  }
//
//  @Test
//  public void testGrantAccess() throws Exception {
//    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
//    RESTClient adminClient = new RESTClient(adminConfig);
//    adminClient.addListener(createRestClientListener());
//
//    NamespaceMeta meta = new NamespaceMeta.Builder().setName(AUTH_NAMESPACE).build();
//    getTestManager(adminConfig, adminClient).createNamespace(meta);
//
//    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
//    RESTClient carolClient = new RESTClient(carolConfig);
//    carolClient.addListener(createRestClientListener());
//
//    ApplicationClient applicationClient = new ApplicationClient(carolConfig, carolClient);
//    NamespaceId namespaceId = new NamespaceId(AUTH_NAMESPACE);
//    try {
//      applicationClient.list(namespaceId);
//      Assert.fail();
//    } catch (IOException ex) {
//      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
//    }
//    // Now authorize the user to access the namespace
//    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
//    authorizationClient.grant(namespaceId, new Principal(CAROL, USER), Collections.singleton(Action.READ));
//    applicationClient.list(namespaceId);
//    // Now delete the namespace and make sure that it is deleted
//    getNamespaceClient().delete(namespaceId);
//    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
//  }
//
//  @Test
//  public void testDeployApp() throws Exception {
//    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
//    RESTClient adminClient = new RESTClient(adminConfig);
//    adminClient.addListener(createRestClientListener());
//    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
//    authorizationClient.grant(NamespaceId.DEFAULT, new Principal(BOB, USER), Collections.singleton(Action.WRITE));
//
//    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
//    RESTClient bobClient = new RESTClient(bobConfig);
//    bobClient.addListener(createRestClientListener());
//
//    getTestManager(bobConfig, bobClient).deployApplication(NamespaceId.DEFAULT, PurchaseApp.class);
//  }
//
//  @Test
//  public void testDeployAppUnauthorized() throws Exception {
//    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
//    RESTClient adminClient = new RESTClient(adminConfig);
//    adminClient.addListener(createRestClientListener());
//
//    NamespaceMeta meta = new NamespaceMeta.Builder().setName(AUTH_NAMESPACE).build();
//    getTestManager(adminConfig, adminClient).createNamespace(meta);
//
//    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
//    RESTClient aliceClient = new RESTClient(aliceConfig);
//    aliceClient.addListener(createRestClientListener());
//
//    try {
//      getTestManager(aliceConfig, aliceClient).deployApplication(new NamespaceId(AUTH_NAMESPACE), PurchaseApp.class);
//      Assert.fail();
//    } catch (Exception ex) {
//      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
//    } finally {
//      // Now delete the namespace and make sure that it is deleted
//      getNamespaceClient().delete(new NamespaceId(AUTH_NAMESPACE));
//      Assert.assertFalse(getNamespaceClient().exists(new NamespaceId(AUTH_NAMESPACE)));
//    }
//  }
//
//  @Test
//  // https://issues.cask.co/browse/CDAP-7680 Prevents us from deleting privileges.
//  public void testCreatedDeletedPrivileges() throws Exception {
//    // Create a namespace
//    NamespaceMeta meta = new NamespaceMeta.Builder().setName(AUTH_NAMESPACE).build();
//    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
//    RESTClient adminClient = new RESTClient(adminConfig);
//    adminClient.addListener(createRestClientListener());
//    getTestManager(adminConfig, adminClient).createNamespace(meta);
//    // Verify that the user has all the privileges on the created namespace
//    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
//    Principal adminPrincipal = new Principal(ADMIN_USER, USER);
//    Set<Privilege> listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
//    int count = 0;
//    for (Privilege listPrivilege : listPrivileges) {
//      if (listPrivilege.getEntity().getEntityName().equals(AUTH_NAMESPACE)) {
//        count++;
//      }
//    }
//    Assert.assertEquals(4, count);
//
//    // Now delete the namespace and make sure that it is deleted
//    getNamespaceClient().delete(new NamespaceId(AUTH_NAMESPACE));
//    Assert.assertFalse(getNamespaceClient().exists(new NamespaceId(AUTH_NAMESPACE)));
//
//    // Check if the privileges are deleted
//    listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
//    count = 0;
//    for (Privilege listPrivilege : listPrivileges) {
//      if (listPrivilege.getEntity().getEntityName().equals(AUTH_NAMESPACE)) {
//        count++;
//      }
//    }
//    Assert.assertEquals(0, count);
//  }
//
//  @Test
//  // Grant a user READ access on a dataset. Try to get the dataset from a program and call a WRITE method on it.
//  public void testWriteWithReadAuth() throws Exception {
//    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
//    RESTClient adminClient = new RESTClient(adminConfig);
//    adminClient.addListener(createRestClientListener());
//
//    DatasetClient datasetAdminClient = new DatasetClient(adminConfig, adminClient);
//    DatasetId testDatasetinstance = NamespaceId.DEFAULT.dataset("testWriteDataset");
//    datasetAdminClient.create(testDatasetinstance, "table");
//    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
//    authorizationClient.grant(NamespaceId.DEFAULT, new Principal(EVE, USER),
//                              Collections.singleton(Action.READ));
//    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
//    RESTClient eveClient = new RESTClient(eveConfig);
//    eveClient.addListener(createRestClientListener());
//    DatasetClient datasetClient = new DatasetClient(eveConfig, eveClient);
//    try {
//      datasetClient.truncate(testDatasetinstance);
//      Assert.fail();
//    } catch (UnauthorizedException ex) {
//      // Expected
//    }
//  }

  @Test
  public void testCrossNsWithAuth() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    TestManager adminTestManager = getTestManager(adminConfig, adminClient);
    DatasetClient adminDatasetClient = new DatasetClient(adminConfig, adminClient);


    // create a auth namespace
    NamespaceMeta meta = new NamespaceMeta.Builder().setName("authNS2").build();

    adminTestManager.createNamespace(meta);

    adminTestManager.deployApplication(meta.getNamespaceId(), DatasetCrossNSAccessWithMRApp.class);

    // give BOB ALL permission in the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(meta.getNamespaceId(), new Principal(BOB, USER), EnumSet.allOf(Action.class));


//    testCrossNSSystemDatasetAccessWithAuthMapReduce(mrManager);
    testCrossNSDatasetAccessWithAuthMapReduce(adminTestManager, adminConfig, adminClient);

    getNamespaceClient().delete(meta.getNamespaceId());

  }

  private void testCrossNSDatasetAccessWithAuthMapReduce(TestManager testManager,
                                                         ClientConfig adminConfig, RESTClient adminClient)
    throws Exception {
    NamespaceMeta inputDatasetNS = new NamespaceMeta.Builder().setName("inputNS").build();
    testManager.createNamespace(inputDatasetNS);
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputNS").build();
    testManager.createNamespace(outputDatasetNS);
//    testManager.addDatasetInstance("keyValueTable", inputDatasetNS.getNamespaceId().dataset("table1"));
    testManager.addDatasetInstance("keyValueTable", outputDatasetNS.getNamespaceId().dataset("table2"));

    addDummyData(inputDatasetNS.getNamespaceId().dataset("table1"));

    Map<String, String> argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMRApp.INPUT_DATASET_NS, inputDatasetNS.getNamespaceId().getNamespace(),
      DatasetCrossNSAccessWithMRApp.INPUT_DATASET_NAME, "table1",
      DatasetCrossNSAccessWithMRApp.OUTPUT_DATASET_NS, outputDatasetNS.getNamespaceId().getNamespace(),
      DatasetCrossNSAccessWithMRApp.OUTPUT_DATASET_NAME, "table2");

    // Switch to BOB and run the  mapreduce job. The job will fail at the runtime since BOB does not have permission
    // on the input and output datasets in another namespaces.

    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());


    ProgramClient bobProgramClient = new ProgramClient(bobConfig, bobClient);

        AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal adminPrincipal = new Principal(BOB, USER);
    Set<Privilege> listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    for (Privilege listPrivilege : listPrivileges) {
//      if (listPrivilege.getEntity().getEntityName().equals(AUTH_NAMESPACE)) {
//        count++;
//      }
      System.out.println("### priv " + listPrivilege);
    }


    assertProgramFailure(argsForMR, bobProgramClient,
                         new NamespaceId("authNS2").app(DatasetCrossNSAccessWithMRApp.class.getSimpleName())
                           .program(ProgramType.MAPREDUCE, DatasetCrossNSAccessWithMRApp.MAPREDUCE_PROGRAM));

//    // Switch back to Alice
//    SecurityRequestContext.setUserId(ALICE.getName());
//    // Verify nothing write to the output dataset
//    assertDatasetIsEmpty(outputDatasetNS.getNamespaceId(), "table2");
//
//    // give privilege to BOB on the input dataset
//    grantAndAssertSuccess(inputDatasetNS.getNamespaceId().dataset("table1"), BOB, EnumSet.of(Action.READ));
//
//    // switch back to bob and try running again. this will still fail since bob does not have access on the output
//    // dataset
//    SecurityRequestContext.setUserId(BOB.getName());
//    assertProgramFailure(argsForMR, mrManager);
//
//    // Switch back to Alice
//    SecurityRequestContext.setUserId(ALICE.getName());
//    // Verify nothing write to the output dataset
//    assertDatasetIsEmpty(outputDatasetNS.getNamespaceId(), "table2");
//
//    // give privilege to BOB on the output dataset
//    grantAndAssertSuccess(outputDatasetNS.getNamespaceId().dataset("table2"), BOB, EnumSet.of(Action.WRITE));
//
//    // switch back to BOB and run MR again. this should work
//    SecurityRequestContext.setUserId(BOB.getName());
//    mrManager.start(argsForMR);
//    mrManager.waitForFinish(5, TimeUnit.MINUTES);
//
//    // Verify results as alice
//    SecurityRequestContext.setUserId(ALICE.getName());
//    verifyDummyData(outputDatasetNS.getNamespaceId(), "table2");
    testManager.deleteDatasetInstance(inputDatasetNS.getNamespaceId().dataset("table1"));
    testManager.deleteDatasetInstance(outputDatasetNS.getNamespaceId().dataset("table2"));

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(inputDatasetNS.getNamespaceId());
    getNamespaceClient().delete(outputDatasetNS.getNamespaceId());
    Assert.assertFalse(getNamespaceClient().exists(inputDatasetNS.getNamespaceId()));
    Assert.assertFalse(getNamespaceClient().exists(outputDatasetNS.getNamespaceId()));
  }


  private void addDummyData(DatasetId datasetId) throws Exception {
    DataSetManager<KeyValueTable> kvTableDataset = getKVTableDataset(datasetId);
    KeyValueTable inputTable = kvTableDataset.get();
    inputTable.write("hello", "world");
    kvTableDataset.flush();
  }

  private void assertProgramFailure(Map<String, String> programArgs,
                                                               final ProgramClient programClient,
                                                               final ProgramId programId)
    throws ProgramNotFoundException, UnauthenticatedException, UnauthorizedException, IOException, TimeoutException,
    InterruptedException, ExecutionException {
    programClient.start(programId, false, programArgs);
    programClient.waitForStatus(programId, ProgramStatus.STOPPED, 5, TimeUnit.MINUTES);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // verify program history just have failures
        List<RunRecord> history = programClient.getAllProgramRuns(programId, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
        for (final RunRecord runRecord : history) {
          if (runRecord.getStatus() != ProgramRunStatus.FAILED) {
            return false;
          }
        }
        return true;
      }
    }, 30, TimeUnit.SECONDS, "Not all program runs have failed status. Expected all run status to be failed");
  }
}
