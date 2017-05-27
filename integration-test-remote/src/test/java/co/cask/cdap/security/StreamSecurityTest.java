package co.cask.cdap.security;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import java.util.concurrent.TimeoutException;
import java.lang.InterruptedException;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.lang.Exception;

import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.AuthorizationClient;
import java.util.Collections;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import static co.cask.cdap.proto.security.Principal.PrincipalType.USER;

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

public class StreamSecurityTest extends AudiTestBase {
  private static final StreamId NONEXISTENT_STREAM = TEST_NAMESPACE.stream("nonexistentStream");
  private static final StreamId STREAM_NAME = TEST_NAMESPACE.stream("streamTest");

  private static final String ADMIN_USER = "cdapitn";
  private static final String ALICE = "alice";
  private static final String BOB = "bob";
  private static final String CAROL = "carol";
  private static final String EVE = "eve";
  private static final String PASSWORD_SUFFIX = "password";
  private static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";

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
   * SEC-AUTH-008's version of SEC-AUTH-019
   * Grant a user READ access on a dataset. Try to get the dataset from a program and call a WRITE method on it.
   * @throws Exception
   */
  @Test
  public void SEC_AUTH_019() throws Exception {

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

    //create admin client
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
      // Expected
    }

    // Now delete the namespace and make sure that it is deleted
    getNamespaceClient().delete(namespaceId);
    Assert.assertFalse(getNamespaceClient().exists(namespaceId));
  }

  @Test
  @Ignore
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

  @Test
  @Ignore
  public void testNonexistentStreams() throws Exception {
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());

    // test interaction with nonexistent stream; should fail
    try {
      streamClient.getConfig(NONEXISTENT_STREAM);
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
      // expected
    }
    try {
      streamClient.sendEvent(NONEXISTENT_STREAM, "testEvent");
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
      // expected
    }
    try {
      streamClient.getEvents(NONEXISTENT_STREAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                             Lists.<StreamEvent>newArrayList());
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
      // expected
    }
    try {
      streamClient.truncate(NONEXISTENT_STREAM);
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
      // expected
    }

    // creation with invalid characters should fail
    try {
      createStream(STREAM_NAME.getStream() + "&");
      Assert.fail();
    } catch (BadRequestException expected) {
      // expected
    }
    try {
      createStream(STREAM_NAME.getStream() + ".");
      Assert.fail();
    } catch (BadRequestException expected) {
      // expected
    }
  }

  // We have to use RestClient directly to attempt to create a stream with an invalid name (negative test against the
  // StreamHandler), because the StreamClient throws an IllegalArgumentException when passing in an invalid stream name.
  private void createStream(String streamName)
    throws BadRequestException, IOException, UnauthenticatedException, UnauthorizedException {
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveNamespacedURLV3(TEST_NAMESPACE, String.format("streams/%s", streamName));
    HttpResponse response = getRestClient().execute(HttpMethod.PUT, url, clientConfig.getAccessToken(),
                                                    HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + response.getResponseBodyAsString());
    }
  }
}