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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

public class StreamSecurityTest extends AudiTestBase {
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
   * SEC-AUTH-019(STREAM)'s version of SEC-AUTH-008
   * Grant a user READ access on a stream. Try to get the stream from a program and call a WRITE method on it.
   * @throws Exception
   */
  @Test
  public void secAuth008() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //create user CAROL
    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    //create carol client
    StreamClient streamCarolClient = new StreamClient(carolConfig, carolClient);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);

    //creating a stream using admin client
    streamAdminClient.create(STREAM_NAME);
    StreamProperties config = streamAdminClient.getConfig(STREAM_NAME);
    Assert.assertNotNull(config);

    //now authorize the user READ access to the STREAM
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(STREAM_NAME, new Principal(CAROL, USER), Collections.singleton(Action.READ));

    //calling a write method should fail
    try {
      streamCarolClient.sendEvent(STREAM_NAME, " a b ");
      Assert.fail();
    } catch (UnauthorizedException ex) {
      //Expected
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * SEC-AUTH-019(STREAM)'s version of SEC-AUTH-009
   * Grant a user WRITE access on a stream. Try to get the stream from a program and call a READ method on it.
   * @throws Exception
   */
  @Test
  public void secAuth009() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //create user CAROL
    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    //create carol client
    StreamClient streamCarolClient = new StreamClient(carolConfig, carolClient);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);

    //creating a stream using admin client
    streamAdminClient.create(STREAM_NAME);
    StreamProperties config = streamAdminClient.getConfig(STREAM_NAME);
    Assert.assertNotNull(config);

    //now authorize the user READ access to the STREAM
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(STREAM_NAME, new Principal(CAROL, USER), Collections.singleton(Action.WRITE));

    //using admin to send message down the stream
    streamAdminClient.sendEvent(STREAM_NAME, " a b ");

    //calling a read method should fail, since carol only has WRITE privilege
    try {
      List<StreamEvent> events = streamCarolClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                             Lists.<StreamEvent>newArrayList());
      Assert.fail();
    } catch (IOException ex) {
      //Expected
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * SEC-AUTH-019(STREAM)'s version of SEC-AUTH-012
   * Grant a user READ access on a stream. Try to get the stream from a program and call a READ method on it.
   * @throws Exception
   */
  @Test
  public void secAuth012() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //create user CAROL
    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    //create carol client
    StreamClient streamCarolClient = new StreamClient(carolConfig, carolClient);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);

    //creating a stream using admin client
    streamAdminClient.create(STREAM_NAME);
    StreamProperties config = streamAdminClient.getConfig(STREAM_NAME);
    Assert.assertNotNull(config);

    //now authorize the user READ access to the STREAM
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(STREAM_NAME, new Principal(CAROL, USER), Collections.singleton(Action.READ));

    //using admin to send message down the stream
    streamAdminClient.sendEvent(STREAM_NAME, " a b ");

    //calling a read method should success, since carol has READ privilege
    List<StreamEvent> events = streamCarolClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(" a b ", Bytes.toString(events.get(0).getBody()));

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * SEC-AUTH-019(STREAM)'s version of SEC-AUTH-013
   * Grant a user WRITE access on a stream. Try to get the stream from a program and call a WRITE method on it.
   * @throws Exception
   */
  @Test
  public void secAuth013() throws Exception {

    //creating an adminClient
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    //creating namespace with random name
    String name = generateRandomName();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(name).build();
    getTestManager(adminConfig, adminClient).createNamespace(meta);

    //create user CAROL
    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    //create carol client
    StreamClient streamCarolClient = new StreamClient(carolConfig, carolClient);

    //start of client code here:
    StreamClient streamAdminClient = new StreamClient(adminConfig, adminClient);
    NamespaceId namespaceId = new NamespaceId(name);

    //creating a stream using admin client
    streamAdminClient.create(STREAM_NAME);
    StreamProperties config = streamAdminClient.getConfig(STREAM_NAME);
    Assert.assertNotNull(config);

    //now authorize carol WRITE access to the STREAM
    AuthorizationClient authorizationClient = new AuthorizationClient(adminConfig, adminClient);
    authorizationClient.grant(STREAM_NAME, new Principal(CAROL, USER), Collections.singleton(Action.WRITE));

    //using the user carol to write message on the stream, should succeed
    streamCarolClient.sendEvent(STREAM_NAME, " a b ");

    //calling a read method from admin client should generate expected result,
    //since carol successfully write to the stream and admin can retrieve it
    List<StreamEvent> events = streamAdminClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());

    //Asserting what Carol read from stream matches what Admin put inside stream.
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(" a b ", Bytes.toString(events.get(0).getBody()));

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  /**
   * generic stream read and write test without user authorization.
   * @throws Exception
   */
  @Test
  public void testStreams() throws Exception {
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());

    streamClient.create(STREAM_NAME);
    StreamProperties config = streamClient.getConfig(STREAM_NAME);
    Assert.assertNotNull(config);

    // create is idempotent
    streamClient.create(STREAM_NAME);

    streamClient.sendEvent(STREAM_NAME, "");
    streamClient.sendEvent(STREAM_NAME, " a b ");


    List<StreamEvent> events = streamClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                      Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(2, events.size());
    Assert.assertEquals("", Bytes.toString(events.get(0).getBody()));
    Assert.assertEquals(" a b ", Bytes.toString(events.get(1).getBody()));

    Map<String, String> streamTags =
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, STREAM_NAME.getNamespace(),
                      Constants.Metrics.Tag.STREAM, STREAM_NAME.getStream());
    /* commented out metrics for disabling security cache */
//    checkMetric(streamTags, "system.collect.events", 2, 10);
//    checkMetric(streamTags, "system.collect.bytes", 5, 10);

    streamClient.truncate(STREAM_NAME);
    events = streamClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                    Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(0, events.size());
  }

}
