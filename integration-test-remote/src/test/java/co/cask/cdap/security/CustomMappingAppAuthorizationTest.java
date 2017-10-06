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
 * App Authorization test for custom mapping
 */
public class CustomMappingAppAuthorizationTest extends BasicAppAuthorizationTest {

  @Before
  public void setup() throws Exception {
    super.setup();
    // Note: authorization is a reserve keyword in hive so a dataset with that name cannot be created hence authz
    testNamespace =
      getNamespaceMeta(testNamespace.getNamespaceId(), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization", "authorization", "authz");
    namespaceMeta1 =
      getNamespaceMeta(namespaceMeta1.getNamespaceId(), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization1", "authorization1", "authz1");
    namespaceMeta2 =
      getNamespaceMeta(namespaceMeta2.getNamespaceId(), BOB, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(BOB, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization2", "authorization2", "authz2");
    appOwner1 = null;
    appOwner2 = null;
  }
}
