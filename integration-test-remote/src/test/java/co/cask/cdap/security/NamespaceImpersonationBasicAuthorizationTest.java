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

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *  Basic authorization test for namespace level impersonation
 */
public class NamespaceImpersonationBasicAuthorizationTest extends BasicAuthorizationTest {

  @Before
  public void setup() throws Exception {
    testNamespace =
      getNamespaceMeta(testNamespace.getNamespaceId(), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                       null, null);
  }

  /**
   * Test creating the same namespace(create -> delete -> create) with different owners
   * This is to test the ugi cache is providing the correct ugi to create the namespace. If a namespace is created with
   * some owner and then get deleted, recreating the namespace with same name but different owner should check the
   * permission on the new owner.
   *
   * Note that this test requires alice has the corrospending privileges on creating namespace on hbase, hdfs and hive,
   * eve does not have corresponding privileges on creating namespaces on hbase, hdfs or hive.
   */
  @Test
  public void testCreateNamespaceWithDifferentOwners() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    // pre-grant required privileges
    authorizationTestClient.grant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    authorizationTestClient.grant(ADMIN_USER, new KerberosPrincipalId(ALICE), Action.ADMIN);
    authorizationTestClient.grant(ADMIN_USER, new KerberosPrincipalId(EVE), Action.ADMIN);

    // initially the namespace owner is ALICE, which is in nscreator group, creation should success
    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
    Assert.assertTrue(getNamespaceClient().exists(namespaceId));
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));

    // change the namespace owner to EVE, EVE is not in nscreator group, so he does not have permission to create
    // namespace in hdfs, creation should fail.
    testNamespace = getNamespaceMeta(testNamespace.getNamespaceId(), EVE, null,
                                     SecurityTestUtils.getKeytabURIforPrincipal(EVE, getMetaClient().getCDAPConfig()),
                                     null, null, null);
    try {
      createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }
}
