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

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AuthTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

/**
 * Integration tests for Authorization. The users here need to be same as in auth.json. The password for the users
 * is their user name suffixed by the word "password".
 */
public class BasicSecurityTest extends AuthTestBase {
  /**
   *  checking if namespace DEFAULT is accessible
   */
  @Test
  public void defaultNamespaceAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    ApplicationClient applicationClient = new ApplicationClient(adminConfig, adminClient);
    // This will fail if getting the list result in exception
    applicationClient.list(NamespaceId.DEFAULT);
  }

  @Test
  public void defaultNamespaceAccessUnauthorized() throws Exception {
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());
    // Check for now Alice doesn't have access to the default namespace's application record list.
    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
    try {
      applicationClient.list(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG_ACCESS.toLowerCase()));
    }
  }

  /**
   * Create a namespace.
   * Check for now Carol doesn't have access to the application record list;
   * Now grant the user read privilege.
   * Check for now Carol has access to application record list;
   * Now delete the namespace.
   * Check the name space has been deleted.
   */
  @Test
  public void testGrantAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String name = "testGrantAccess";
    NamespaceId namespaceId = new NamespaceId(name);
    NamespaceMeta meta = null;
    try {
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
    } catch (NamespaceAlreadyExistsException ex) {
      //if namespace already exists, delete it and create new one
      getNamespaceClient().delete(namespaceId);
      TimeUnit.SECONDS.sleep(1);
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
      TimeUnit.SECONDS.sleep(1);
    }

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(carolConfig, carolClient);
    try {
      applicationClient.list(namespaceId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG_ACCESS.toLowerCase()));
    }
    // Now authorize the user to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(namespaceId, new Principal(CAROL, USER), Collections.singleton(Action.READ));
    // Fail if getting the list result in exception
    applicationClient.list(namespaceId);
    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * Deploy purchase application with client Bob
   * Check if there's anything wrong
   */
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

    // Can't create purchasing app will result in an exception
    getTestManager(bobConfig, bobClient).deployApplication(NamespaceId.DEFAULT, PurchaseApp.class);
  }

  /**
   * Create a namespace, we don't grant alice authorization to write
   * When alice try to deploy an application on the namespace, it will result in failure.
   * Test on whether we recieved such a failure and do clean up on created namespace.
   */
  @Test
  public void testDeployAppUnauthorized() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String name = "testDeployAppUnauthorized";
    NamespaceId namespaceId = new NamespaceId(name);
    NamespaceMeta meta = null;
    try {
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
    } catch (NamespaceAlreadyExistsException ex) {
      //if namespace already exists, delete it and create new one
      getNamespaceClient().delete(namespaceId);
      TimeUnit.SECONDS.sleep(1);
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
      TimeUnit.SECONDS.sleep(1);
    }


    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    try {
      getTestManager(aliceConfig, aliceClient).deployApplication(new NamespaceId(name), PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG_ACCESS.toLowerCase()));
    } finally {
      // Now delete the namespace and make sure that it is deleted
      getNamespaceClient().delete(new NamespaceId(name));
      Assert.assertFalse(getNamespaceClient().exists(new NamespaceId(name)));
    }
  }

  /**
   * Testing that creating namespace will create all 4 privileges on the user
   * Testing that deleting namespace will delete all 4 privileges on the user
   */
  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    // Create a namespace
    String name = "testCreatedDeletedPrivileges";
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    NamespaceId namespaceId = new NamespaceId(name);
    meta = null;
    try {
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
    } catch (NamespaceAlreadyExistsException ex) {
      //if namespace already exists, delete it and create new one
      getNamespaceClient().delete(namespaceId);
      TimeUnit.SECONDS.sleep(1);
      meta = new NamespaceMeta.Builder().setName(name).build();
      getTestManager(adminConfig, adminClient).createNamespace(meta);
      TimeUnit.SECONDS.sleep(1);
    }

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

  /**
   *  Grant a user READ access on a dataset.
   *  Try to get the dataset from a program and call a WRITE method on it.
   */
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

}
