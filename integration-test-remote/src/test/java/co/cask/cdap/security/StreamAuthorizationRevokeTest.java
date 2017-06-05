/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

public class StreamAuthorizationRevokeTest extends AudiTestBase {
  private static final StreamId STREAM_NAME = TEST_NAMESPACE.stream("streamTest");
  private static final String ADMIN_USER = "cdapitn";
  private static final String CAROL = "carol";
  private static final String PASSWORD_SUFFIX = "password";

  // This is to work around https://issues.cask.co/browse/CDAP-7680
  // Where we don't delete privileges when a namespace is deleted.
  private static String generateRandomName() {
    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
    return new BigInteger(130, new SecureRandom()).toString(32);
  }

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException, TimeoutException, Exception {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  /**
   * SEC-CN-014
   * Grant a user access on a dataset/stream in a namespace. Try to access it from a program in a different namespace.
   * Revoke access. Program should start seeing failures after a configured max allowable time
   *
   * In this case, grant a user READ access on a stream. Revoke the READ access and wait for a while.
   * Try to get the stream from a program and call a READ method on it will fail.
   * @throws Exception
   */
  @Test
  public void testRevokeReadAccessFromStream() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //create namespaceId
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);
    //creating stream within the namespace created
    StreamId streamId = namespaceId.stream("streamTest");
    //creating a stream using admin client
    streamAdminClient.create(streamId);
    StreamProperties config = streamAdminClient.getConfig(streamId);
    Assert.assertNotNull(config);

    //create user CAROL
    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    //create carol client
    StreamClient streamCarolClient = new StreamClient(carolConfig, carolClient);

    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);

    //Carol need ADMIN/READ/etc access to stream in order to read from the stream
    authorizationClient.grant(streamId.getNamespaceId(), new Principal(CAROL, USER),
                              Collections.singleton(Action.ADMIN));
    TimeUnit.SECONDS.sleep(10);

    //Grant Carol Read access to stream
    authorizationClient.grant(streamId, new Principal(CAROL, USER), Collections.singleton(Action.READ));
    TimeUnit.SECONDS.sleep(10);

    List<StreamEvent> events = streamCarolClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Revoke Carol READ access from stream, waited 5 mins here for revoke to take effect, 20 secs is not enough
    authorizationClient.revoke(streamId, new Principal(CAROL, USER), Collections.singleton(Action.READ));
    TimeUnit.SECONDS.sleep(300);

    //using admin to send message down the stream
    streamAdminClient.sendEvent(streamId, " a b ");

    //calling a read method should fail, since carol has been revoked the READ privilege from stream
    try {
      events = streamCarolClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                             Lists.<StreamEvent>newArrayList());
      Assert.fail();
    } catch (IOException ex) {
      //Expected
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);

    //resolve unresponsive bug by adding a delay
    TimeUnit.SECONDS.sleep(1);

    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }
}
