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
 *  Basic authorization test for namespace level impersonation
 */
public class NamespaceImpersonationBasicAuthorizationTest extends BasicAuthorizationTestBase {

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    super.setup();
    try {
      testNamespace =
        getNamespaceMeta(new NamespaceId("authorization"), ALICE, null,
                         SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()), null,
                         null, null);
    } catch (Exception e) {
      // should not happen
      Assert.fail("Failed to get keytab url");
    }
  }
}
