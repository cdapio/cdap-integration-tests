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
 * App level impersonation authorization test
 */
public class AppImpersonationAuthorizationTest extends AuthorizationTestBase {

  @Test
  public void testDatasetInProgram() throws Exception {
    // todo: do not use various namespaces names until https://issues.cask.co/browse/CDAP-11985 is fixed.
    testDatasetInProgram(
      getNamespaceMeta(new NamespaceId("appAuth1"), null, null, null, null, null, null),
      getNamespaceMeta(new NamespaceId("appAuth2"), null, null, null, null, null, null),
      ALICE, EVE);
  }

}
