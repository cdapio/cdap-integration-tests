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

import org.junit.Before;

/**
 * App authorization tests for namespace level impersonation
 */
public class NamespaceImpersonationAppAuthorizationTest extends BasicAppAuthorizationTest {

  @Before
  public void setup() throws Exception {
    testNamespace =
      getNamespaceMeta(testNamespace.getNamespaceId(), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                       null, null);
    crossNsTest1 =
      getNamespaceMeta(crossNsTest1.getNamespaceId(), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                       null, null);
    crossNsTest2 =
      getNamespaceMeta(crossNsTest2.getNamespaceId(), BOB, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(BOB, getMetaClient().getCDAPConfig()), null,
                       null, null);
    appOwner1 = null;
    appOwner2 = null;
  }
}
