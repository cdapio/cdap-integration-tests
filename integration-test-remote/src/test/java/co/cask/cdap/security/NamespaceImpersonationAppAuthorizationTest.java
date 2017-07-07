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

import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;

/**
 * App authorization tests for namespace level impersonation
 */
public class NamespaceImpersonationAppAuthorizationTest extends AppAuthorizationTestBase {

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    super.setup();
    try {
      testNamespace =
        getNamespaceMeta(new NamespaceId("authorization"), ALICE, null,
                         SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                         null, null);
      namespaceMeta1 =
        getNamespaceMeta(new NamespaceId("authorization1"), ALICE, null,
                         SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                         null, null);
      namespaceMeta2 =
        getNamespaceMeta(new NamespaceId("authorization2"), BOB, null,
                         SecurityTestUtils.getKeytabURIforPrincipal(BOB, getMetaClient().getCDAPConfig()), null,
                         null, null);
    } catch (Exception e) {
      // should not happen
      Assert.fail("Failed to get keytab url");
    }
    appOwner1 = null;
    appOwner2 = null;
  }
}
