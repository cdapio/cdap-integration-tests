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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Basic test base for authorization, this test base contains tests without impersonation
 *
 * We create a namespace for most of the test cases since we want to make sure the privilege for each user is clean.
 */
public class BasicAuthorizationTest extends AuthorizationTestBase {

  private StreamId streamId = testNamespace.getNamespaceId().stream("testStream");
  private DatasetId testDatasetinstance = testNamespace.getNamespaceId().dataset("testWriteDataset");
  private DatasetTypeId testDatasetTypeId = testNamespace.getNamespaceId().datasetType("table");

  @Override
  public void tearDown() throws Exception {
    // we have to grant ADMIN privileges to all entites such that these entities can be deleted
    userGrant(ADMIN_USER, streamId, Action.ADMIN);
    userGrant(ADMIN_USER, testDatasetinstance, Action.ADMIN);
    userGrant(ADMIN_USER, testDatasetTypeId, Action.ADMIN);
    super.tearDown();
  }

  /**
   * Test the basic grant operations. User should be able to list once he has the privilege on the namespace.
   */
  @Test
  public void testNamespacePrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    NamespaceId namespaceId;

    // ADMIN_USER can't create namespace without having ADMIN privilege on the namespace
    try {
      createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }

    // ADMIN_USER can create namespace after granted privilege on the namespace
    userGrant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
    try {
      // list should fail initially since ALICE does not have privilege on the namespace
      applicationClient.list(namespaceId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }

    // Now authorize ALICE to create a stream
    userGrant(ALICE, streamId, Action.ADMIN);
    StreamClient aliceStreamClient = new StreamClient(aliceConfig, aliceClient);
    aliceStreamClient.create(streamId);

    // cdapitn shouldn't be able to list the stream since he doesn't have privilege on the stream
    StreamClient adminStreamClient = new StreamClient(adminConfig, adminClient);
    List<StreamDetail> streams = adminStreamClient.list(namespaceId);
    Assert.assertEquals(0, streams.size());

    // ADMIN cannot delete the namespace because he doesn't have privileges on the stream
    try {
      getNamespaceClient().delete(namespaceId);
      Assert.fail();
    } catch (IOException ex) {
      // expected
      // TODO: change the error message on cdap platform such that this contains NO_PRIVILEGE_MSG
      // Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }

    // Alice will not able to delete the namespace since she does not have admin on the namespace,
    // even though she has admin on all the entities in the namespace
    try {
      new NamespaceClient(aliceConfig, aliceClient).delete(namespaceId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // expected
    }
    // ADMIN can delete namespace only after ADMIN gets permission to every entity in the namespace
    userGrant(ADMIN_USER, streamId, Action.ADMIN);
    Assert.assertEquals(1, adminStreamClient.list(namespaceId).size());
    getNamespaceClient().delete(namespaceId);
  }

  /**
   * Test list privileges, after creation and deletion, the privilege should remain the same
   */
  @Test
  public void testListPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());


    userGrant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
    Set<Privilege> expected = new HashSet<>();
    expected.add(new Privilege(namespaceId, Action.ADMIN));

    // Verify that the user has all the privileges on the created namespace
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    Principal adminPrincipal = new Principal(ADMIN_USER, Principal.PrincipalType.USER);
    Assert.assertEquals(expected, authorizationClient.listPrivileges(adminPrincipal));

    // add some more privileges
    StreamId streamId = namespaceId.stream("testListPrivilegeStream");
    userGrant(ADMIN_USER, streamId, Action.WRITE);
    expected.add(new Privilege(streamId, Action.WRITE));

    DatasetId datasetId = namespaceId.dataset("testListPrivilegeDataset");
    userGrant(ADMIN_USER, streamId, Action.READ);
    expected.add(new Privilege(datasetId, Action.READ));

    KerberosPrincipalId kerberosPrincipalId = new KerberosPrincipalId("testListPrivilegePrincipal");
    userGrant(ADMIN_USER, kerberosPrincipalId, Action.ADMIN);
    expected.add(new Privilege(kerberosPrincipalId, Action.ADMIN));

    Assert.assertEquals(expected, authorizationClient.listPrivileges(adminPrincipal));


    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));

    // Privileges should stay the same
    Assert.assertEquals(expected, authorizationClient.listPrivileges(adminPrincipal));
  }

  /**
   * Test basic privileges for dataset.
   */
  @Test
  public void testDatasetPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    userGrant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
    DatasetClient datasetAdminClient = new DatasetClient(adminConfig, adminClient);

    // ADMIN_USER creates a dataset that has type "table"
    userGrant(ADMIN_USER, testDatasetinstance, Action.ADMIN);

    // Create, truncate, update should all succeed
    datasetAdminClient.create(testDatasetinstance, "table");
    Assert.assertTrue(datasetAdminClient.exists(testDatasetinstance));
    Assert.assertEquals(1, datasetAdminClient.list(testDatasetinstance.getNamespaceId()).size());
    Assert.assertNotNull(datasetAdminClient.get(testDatasetinstance));

    datasetAdminClient.truncate(testDatasetinstance);
    datasetAdminClient.update(testDatasetinstance, new HashMap<String, String>());

    ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
    RESTClient eveClient = new RESTClient(eveConfig);
    eveClient.addListener(createRestClientListener());
    DatasetClient datasetClient = new DatasetClient(eveConfig, eveClient);

    // EVE can't see the dataset yet
    try {
      datasetClient.exists(testDatasetinstance);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // Expected
    }

    // Now we grant EVE READ privilege on the dataset
    userGrant(EVE, testDatasetinstance, Action.READ);

    // Listing the dataset should succeed
    Assert.assertEquals(true, datasetClient.exists(testDatasetinstance));
    Assert.assertEquals(1, datasetClient.list(testDatasetinstance.getNamespaceId()).size());
    Assert.assertNotNull(datasetClient.get(testDatasetinstance));

    // truncating the dataset should fail
    try {
      datasetClient.truncate(testDatasetinstance);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // Expected
    }

    // updating the dataset should fail
    try {
      datasetClient.update(testDatasetinstance, new HashMap<String, String>());
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // Expected
    }

    // deleting the dataset should fail
    try {
      datasetClient.delete(testDatasetinstance);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // Expected
    }

    // ADMIN_USER should be able to delete the dataset
    datasetAdminClient.delete(testDatasetinstance);
  }

  /**
   * Test list entities on an namespace.
   *
   * This test will only work with list namespaces currently, since to list other entities, we need privileges
   * on the corresponding namespace, and that will make the user be able to list any entity in the namespace.
   */
  @Test
  public void testStreamPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    userGrant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    // Create a stream with Admin
    StreamClient adminStreamClient = new StreamClient(adminConfig, adminClient);
    userGrant(ADMIN_USER, streamId, Action.ADMIN);
    adminStreamClient.create(streamId);
    adminStreamClient.truncate(streamId);
    Assert.assertEquals(1, adminStreamClient.list(testDatasetinstance.getNamespaceId()).size());
    Assert.assertNotNull(adminStreamClient.getConfig(streamId));

    // admin doesn't have WRITE privilege on the stream, so the following actions will fail
    try {
      adminStreamClient.sendEvent(streamId, "an event");
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // expected
    }

    try {
      adminStreamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, new ArrayList<StreamEvent>());
      Assert.fail();
    } catch (Exception ex) {
      // TODO: verify that this contains message from UnauthorizedException
      // Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }

    // Now authorize user Bob to access the stream
    userGrant(BOB, streamId, Action.READ);
    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());
    StreamClient bobStreamClient = new StreamClient(bobConfig, bobClient);

    // Bob can read from but not write to the stream
    bobStreamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, new ArrayList<StreamEvent>());
    try {
      bobStreamClient.sendEvent(streamId, "an event");
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // expected
    }

    // Now authorize user Alice to access the stream
    userGrant(ALICE, streamId, Action.WRITE);
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());
    StreamClient aliceStreamClient = new StreamClient(aliceConfig, aliceClient);

    // Alice can write to but not read from the stream
    aliceStreamClient.sendEvent(streamId, "an event");
    try {
      aliceStreamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, new ArrayList<StreamEvent>());
      Assert.fail();
    } catch (Exception ex) {
      // TODO: verify that this contains message from UnauthorizedException
      // Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }

    // neither Bob nor Alice can drop the stream
    try {
      bobStreamClient.delete(streamId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // expected
    }
    try {
      aliceStreamClient.delete(streamId);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      // expected
    }

    // only admin can drop the stream
    adminStreamClient.delete(streamId);
  }
}
