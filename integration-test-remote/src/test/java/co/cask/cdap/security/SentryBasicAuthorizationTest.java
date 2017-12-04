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

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Role;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Authorization test specifically for sentry
 */
public class SentryBasicAuthorizationTest extends AuthorizationTestBase {
  /**
   * Test role based privileges, note that this test can only be run with sentry extension
   */
  @Test
  public void testRoleBasedStreamPrivileges() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    NamespaceId namespaceId = testNamespace.getNamespaceId();

    String streamName = "testStream";
    String roleName = "streamReadWrite";
    // alice and bob are in group nscreator, eve and carol are not
    String groupName = "nscreator";
    // We need to create different streams to avoid the cache
    List<StreamId> streamLists = new ArrayList<>();
    streamLists.add(namespaceId.stream(streamName + 1));
    streamLists.add(namespaceId.stream(streamName + 2));
    streamLists.add(namespaceId.stream(streamName + 3));

    // pre-grant all the required privileges
    // admin can create the streams and write event to streams
    authorizationTestClient.grant(ADMIN_USER, namespaceId, Action.ADMIN);
    for (StreamId streamId : streamLists) {
      authorizationTestClient.grant(ADMIN_USER, streamId, Action.ADMIN);
      authorizationTestClient.grant(ADMIN_USER, streamId, Action.WRITE);
    }
    String namespacePrincipal = testNamespace.getConfig().getPrincipal();
    if (namespacePrincipal != null) {
      authorizationTestClient.grant(ADMIN_USER, new KerberosPrincipalId(namespacePrincipal), Action.ADMIN);
    }
    ((SentryAuthorizationTestClient) authorizationTestClient).createRole(new Role(roleName));
    // streamId1 is only used to test visibility so only grant EXECUTE
    authorizationTestClient.grant(roleName, streamLists.get(0), Action.EXECUTE, groupName);
    // streamId2 is only allowed to read
    authorizationTestClient.grant(roleName, streamLists.get(1), Action.READ, groupName);
    // streamId3 is only allowed to write
    authorizationTestClient.grant(roleName, streamLists.get(2), Action.WRITE, groupName);

    // Sleep for the cache timeout to let the created role take effect
    authorizationTestClient.waitForAuthzCacheTimeout();

    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    StreamClient adminStreamClient = new StreamClient(adminConfig, adminClient);
    adminStreamClient.create(streamLists.get(0));
    adminStreamClient.sendEvent(streamLists.get(0), "adminTest");

    try {
      // verify namespace list
      ClientConfig eveConfig = getClientConfig(fetchAccessToken(EVE, EVE + PASSWORD_SUFFIX));
      RESTClient eveClient = new RESTClient(eveConfig);
      eveClient.addListener(createRestClientListener());

      NamespaceClient eveNamespaceClient = new NamespaceClient(eveConfig, eveClient);
      Assert.assertTrue(eveNamespaceClient.list().isEmpty());

      ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
      RESTClient bobClient = new RESTClient(bobConfig);
      bobClient.addListener(createRestClientListener());

      NamespaceClient bobNamespaceClient = new NamespaceClient(bobConfig, bobClient);
      Assert.assertEquals(1, bobNamespaceClient.list().size());

      StreamClient bobStreamClient = new StreamClient(bobConfig, bobClient);

      // read and write should fail since bob does not have corresponding privilege
      verifyStreamReadWritePrivilege(bobStreamClient, streamLists.get(0), Collections.<Action>emptySet());

      // create a new stream to avoid the cache
      adminStreamClient.create(streamLists.get(1));
      adminStreamClient.sendEvent(streamLists.get(1), "adminTest");

      // read should success but write should fail
      verifyStreamReadWritePrivilege(bobStreamClient, streamLists.get(1), Collections.singleton(Action.READ));

      // create a new stream to avoid the cache
      adminStreamClient.create(streamLists.get(2));
      adminStreamClient.sendEvent(streamLists.get(2), "adminTest");

      // write should success but read should fail
      verifyStreamReadWritePrivilege(bobStreamClient, streamLists.get(2), Collections.singleton(Action.WRITE));
    } finally {
      authorizationTestClient.revokeAll(roleName);
    }
  }
}
