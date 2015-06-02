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

package co.cask.cdap.apps;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.StreamNotFoundException;
import co.cask.cdap.proto.StreamProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests various methods of a stream such as create, ingest events, fetch events, properties.
 */
public class StreamTest extends AudiTestBase {
  private static final String NONEXISTENT_STREAM = "nonexistentStream";
  private static final String STREAM_NAME = "testStream";

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
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
                      Constants.Metrics.Tag.STREAM, STREAM_NAME);
    checkMetric(streamTags, "system.collect.events", 2, 10);
    checkMetric(streamTags, "system.collect.bytes", 5, 10);

    streamClient.truncate(STREAM_NAME);
    events = streamClient.getEvents(STREAM_NAME, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                    Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(0, events.size());
  }

  @Test
  public void testNonexistentStreams() throws Exception {
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());

    // test interaction with nonexistent stream; should fail
    try {
      streamClient.getConfig(NONEXISTENT_STREAM);
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.sendEvent(NONEXISTENT_STREAM, "testEvent");
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.getEvents(NONEXISTENT_STREAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                             Lists.<StreamEvent>newArrayList());
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.truncate(NONEXISTENT_STREAM);
      Assert.fail(String.format("Expected '%s' to not exist.", NONEXISTENT_STREAM));
    } catch (StreamNotFoundException expected) {
    }

    // creation with invalid characters should fail
    try {
      streamClient.create(STREAM_NAME + "&");
      Assert.fail();
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(
        "Stream name can only contain alphanumeric, '-' and '_' characters"));
    }
    try {
      streamClient.create(STREAM_NAME + ".");
      Assert.fail();
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(
        "Stream name can only contain alphanumeric, '-' and '_' characters"));
    }
  }
}
