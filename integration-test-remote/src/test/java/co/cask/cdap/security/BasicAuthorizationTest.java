/*
 * Copyright © 2016 Cask Data, Inc.
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
 * Integration tests for Authorization. The users here need to be same as in auth.json. The password for the users
 * is their user name suffixed by the word "password".
 *
 * We create a namespace for most of the test cases since we want to make sure the privilege for each user is clean.
 */
public class BasicAuthorizationTest extends AuthorizationTestBase {

  @Test
  public void testGrantAccess() throws Exception {
    testGrantAccess(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDeployApp() throws Exception {
    testDeployApp(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDeployAppUnauthorized() throws Exception {
    testDeployAppUnauthorized(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    testCreatedDeletedPrivileges(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testWriteWithReadAuth() throws Exception {
    testWriteWithReadAuth(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDatasetInProgram() throws Exception {
    testDatasetInProgram(getNamespaceMeta(new NamespaceId("auth1"), null, null, null, null, null, null),
                         getNamespaceMeta(new NamespaceId("auth2"), null, null, null, null, null, null));
  }

  @Test
  public void testListEntities() throws Exception {
    testListEntities(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }
}
