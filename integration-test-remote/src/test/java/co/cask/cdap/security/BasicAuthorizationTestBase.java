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
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Basic test base for authorization, this test base contains tests without impersonation
 *
 * We create a namespace for most of the test cases since we want to make sure the privilege for each user is clean.
 */
public class BasicAuthorizationTestBase extends AuthorizationTestBase {

  /**
   * Test the basic grant operations. User should be able to list once he has the privilege on the namespace.
   */
  @Test
  public void testBasicGrantOperations() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(carolConfig, carolClient);
    try {
      // initially list should fail since carol does not have privilege on the namespace
      applicationClient.list(namespaceId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
    // Now authorize the user to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(namespaceId, new Principal(CAROL, Principal.PrincipalType.USER),
                              Collections.singleton(Action.READ));
    applicationClient.list(namespaceId);
  }

  /**
   * Test after creation of an entity we get all privileges on it and make sure privileges are removed once the entity
   * is deleted.
   */
  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    // Verify that the user has all the privileges on the created namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal adminPrincipal = new Principal(ADMIN_USER, Principal.PrincipalType.USER);
    Set<Privilege> listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    int count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(namespaceId.getEntityName())) {
        count++;
      }
    }
    Assert.assertEquals(4, count);

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));

    // Check if the privileges are deleted
    listPrivileges = authorizationClient.listPrivileges(adminPrincipal);
    count = 0;
    for (Privilege listPrivilege : listPrivileges) {
      if (listPrivilege.getEntity().getEntityName().equals(namespaceId.getEntityName())) {
        count++;
      }
    }
    Assert.assertEquals(0, count);
  }

  /**
   * Test basic privileges for dataset.
   */
  @Test
  public void testWriteWithReadAuth() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    DatasetClient datasetAdminClient = new DatasetClient(adminConfig, adminClient);
    DatasetId testDatasetinstance = namespaceId.dataset("testWriteDataset");
    datasetAdminClient.create(testDatasetinstance, "table");
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(namespaceId, new Principal(EVE, Principal.PrincipalType.USER),
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

  /**
   * Test list entities on an namespace.
   *
   * This test will only work with list namespaces currently, since to list other entities, we need privileges
   * on the corresponding namespace, and that will make the user be able to list any entity in the namespace.
   */
  @Test
  public void testListEntities() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    // Now authorize user carol to access the namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(namespaceId, new Principal(CAROL, Principal.PrincipalType.USER),
                              Collections.singleton(Action.WRITE));

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());
    NamespaceClient carolNamespaceClient = new NamespaceClient(carolConfig, carolClient);

    // Carol should only be able to see the given namespace
    List<NamespaceMeta> namespaces = carolNamespaceClient.list();
    Assert.assertEquals(1, namespaces.size());
    Assert.assertEquals(namespaceId.getEntityName(), namespaces.iterator().next().getName());

    // Create a stream with carol
    StreamId streamId = namespaceId.stream("testStream");
    StreamClient carolStreamClient = new StreamClient(carolConfig, carolClient);
    carolStreamClient.create(streamId);

    // cdapitn should be able to list the stream since he has privileges on the namespace
    StreamClient adminStreamClient = new StreamClient(adminConfig, adminClient);
    List<StreamDetail> streams = adminStreamClient.list(namespaceId);
    Assert.assertEquals(1, namespaces.size());
    Assert.assertEquals(streamId.getEntityName(), streams.iterator().next().getName());

    AuthorizationClient carolAuthorizationClient = new AuthorizationClient(carolConfig, carolClient);
    // simply grant READ on eve will not let Eve list the stream since Eve does not have privilege on the namespace
    carolAuthorizationClient.grant(streamId, new Principal(EVE, Principal.PrincipalType.USER),
                                   Collections.singleton(Action.READ));

    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());

    StreamClient eveStreamClient = new StreamClient(eveConfig, eveClient);
    try {
      // Eve should not be able to list the streams since Eve does not have privilege on the namespace
      eveStreamClient.list(namespaceId);
      Assert.fail();
    } catch (Exception ex) {
      // expected
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  /**
   * Test delete namespace with two different clients, deletion should work for both clients
   */
  @Test
  public void testDeleteNamespaceWithDifferentClients() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    EntityId instanceId = new InstanceId("cdap");
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);

    try {
      authorizationClient.grant(instanceId, new Principal(ALICE, Principal.PrincipalType.USER),
                                Collections.singleton(Action.ADMIN));
      authorizationClient.grant(instanceId, new Principal(BOB, Principal.PrincipalType.USER),
                                Collections.singleton(Action.ADMIN));

      createAndDeleteNamespace(testNamespace, ALICE);
      createAndDeleteNamespace(testNamespace, BOB);
    } finally {
      authorizationClient.revoke(instanceId, new Principal(ALICE, Principal.PrincipalType.USER),
                                 Collections.singleton(Action.ADMIN));
      authorizationClient.revoke(instanceId, new Principal(BOB, Principal.PrincipalType.USER),
                                 Collections.singleton(Action.ADMIN));
    }
  }

  private void createAndDeleteNamespace(NamespaceMeta namespaceMeta, String user) throws Exception {
    ClientConfig clientConfig = getClientConfig(fetchAccessToken(user, user + PASSWORD_SUFFIX));
    RESTClient client = new RESTClient(clientConfig);
    client.addListener(createRestClientListener());

    // create namespace with client
    createAndRegisterNamespace(namespaceMeta, clientConfig, client);

    // verify namespace exists
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig, client);
    Assert.assertTrue(namespaceClient.exists(namespaceMeta.getNamespaceId()));

    // delete it and verify it is gone
    namespaceClient.delete(namespaceMeta.getNamespaceId());
    Assert.assertFalse(namespaceClient.exists(namespaceMeta.getNamespaceId()));
  }
}
