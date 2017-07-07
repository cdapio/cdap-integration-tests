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

import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Test;

/**
 * Namespace level impersonation authorization test
 */
public class NamespaceImpersonationAuthorizationTest extends AuthorizationTestBase {
  // todo: remove this and use variable in test base until https://issues.cask.co/browse/CDAP-11985 is fixed.
  private static final NamespaceId TEST_NAMESPACE = new NamespaceId("namespaceImp");

  @Test
  public void testGrantAccess() throws Exception {
    testBasicGrantOperations(
      getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null));
  }

  @Test
  public void testDeployApp() throws Exception {
    testDeployApp(getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                                   SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                                   null, null, null));
  }

  @Test
  public void testDeployAppUnauthorized() throws Exception {
    testDeployAppUnauthorized(
      getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null));
  }

  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    testCreatedDeletedPrivileges(
      getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null));
  }

  @Test
  public void testWriteWithReadAuth() throws Exception {
    testWriteWithReadAuth(
      getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null));
  }

  @Test
  public void testDatasetInProgram() throws Exception {
    // todo: do not use various namespaces names until https://issues.cask.co/browse/CDAP-11985 is fixed.
    testDatasetInProgram(
      getNamespaceMeta(new NamespaceId("namespaceAuth1"), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null),
      getNamespaceMeta(new NamespaceId("namespaceAuth2"), EVE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(EVE, getMetaClient().getCDAPConfig()),
                       null, null, null),
      null, null);
  }

  @Test
  public void testListEntities() throws Exception {
    testListEntities(
      getNamespaceMeta(TEST_NAMESPACE, ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       null, null, null));
  }
}
