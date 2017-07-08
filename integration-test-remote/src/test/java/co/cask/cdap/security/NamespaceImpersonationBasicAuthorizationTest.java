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
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *  Basic authorization test for namespace level impersonation
 */
public class NamespaceImpersonationBasicAuthorizationTest extends BasicAuthorizationTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();
    testNamespace =
      getNamespaceMeta(new NamespaceId("authorization"), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                       null, null);
  }

  /**
   * Test creating the same namespace(create -> delete -> create) with different owners
   *
   * Note that this test requires alice has the corrospending privileges on creating namespace on hbase, hdfs and hive,
   * eve does not have corresponding privileges on creating namespaces on hbase, hdfs or hive.
   */
  @Test
  public void testCreateNamespaceWithDifferentOwners() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    // should success
    NamespaceId namespaceId = createAndRegisterNamespace(testNamespace, adminConfig, adminClient);
    Assert.assertTrue(getNamespaceClient().exists(namespaceId));
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));

    testNamespace = getNamespaceMeta(new NamespaceId("authorization"), EVE, null,
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
