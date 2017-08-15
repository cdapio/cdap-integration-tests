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
import org.junit.Before;

/**
 * App Authorization test for custom mapping
 */
public class CustomMappingAppAuthorizationTest extends AppAuthorizationTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();
    testNamespace =
      getNamespaceMeta(new NamespaceId("authorization"), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization", "authorization", "authorization");
    namespaceMeta1 =
      getNamespaceMeta(new NamespaceId("authorization1"), ALICE, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization1", "authorization1", "authorization1");
    namespaceMeta2 =
      getNamespaceMeta(new NamespaceId("authorization2"), BOB, null,
                       SecurityTestUtils.getKeytabURIforPrincipal(BOB, getMetaClient().getCDAPConfig()),
                       "/cdap/authorization2", "authorization2", "authorization2");
    appOwner1 = null;
    appOwner2 = null;
  }
}
