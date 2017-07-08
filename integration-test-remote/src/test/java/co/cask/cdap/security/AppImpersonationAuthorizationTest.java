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
 * App authorization tests for app level impersonation
 */
public class AppImpersonationAuthorizationTest extends AppAuthorizationTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();
    testNamespace = getNamespaceMeta(new NamespaceId("authorization"), ALICE, "deployers",
                                     SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()),
                                     null, null, null);
    appOwner = EVE;
  }
}
