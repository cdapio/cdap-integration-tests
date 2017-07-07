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

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for basic authorization. The users here need to be same as in auth.json.
 * The password for the users is their user name suffixed by the word "password".
 */
public class BasicAuthorizationTest extends AuthorizationTestBase {
  // todo: move this to test base until https://issues.cask.co/browse/CDAP-11985 is fixed.
  private static final NamespaceId TEST_NAMESPACE = new NamespaceId("authorization");

  @Test
  public void testDefaultNamespaceAccess() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    ApplicationClient applicationClient = new ApplicationClient(adminConfig, adminClient);
    applicationClient.list(NamespaceId.DEFAULT);
  }

  @Test
  public void testDefaultNamespaceAccessUnauthorized() throws Exception {
    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    ApplicationClient applicationClient = new ApplicationClient(aliceConfig, aliceClient);
    try {
      applicationClient.list(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (UnauthorizedException ex) {
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NO_PRIVILEGE_MSG.toLowerCase()));
    }
  }

  @Test
  public void testGrantAccess() throws Exception {
    testBasicGrantOperations(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
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
  public void testListEntities() throws Exception {
    testListEntities(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }
}
