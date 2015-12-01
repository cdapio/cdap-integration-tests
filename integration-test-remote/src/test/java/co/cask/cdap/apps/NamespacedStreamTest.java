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
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests isolation of streams across namespaces.
 */
public class NamespacedStreamTest extends AudiTestBase {
  private static final Id.Namespace NS1 = Id.Namespace.from("ns1");
  private static final Id.Namespace NS2 = Id.Namespace.from("ns2");

  @Test
  public void testNamespacedStreams() throws Exception {
    NamespaceClient namespaceClient = new NamespaceClient(getClientConfig(), getRestClient());
    namespaceClient.create(new NamespaceMeta.Builder().setName(NS1).build());
    namespaceClient.create(new NamespaceMeta.Builder().setName(NS2).build());

    StreamClient streamClient = getStreamClient();

    // Both namespaces should start with 0 streams
    Assert.assertTrue(streamClient.list(NS1).isEmpty());
    Assert.assertTrue(streamClient.list(NS2).isEmpty());

    String streamName = "namespacedStream";
    Id.Stream streamId1 = Id.Stream.from(NS1, streamName);
    Id.Stream streamId2 = Id.Stream.from(NS2, streamName);
    streamClient.create(streamId1);

    // namespace2 still has 0 streams after creating namespace in first namespace
    List<StreamDetail> ns1Streams = streamClient.list(NS1);
    Assert.assertEquals(1, ns1Streams.size());
    Assert.assertEquals(streamName, ns1Streams.get(0).getName());
    Assert.assertTrue(streamClient.list(NS2).isEmpty());

    streamClient.create(streamId2);
    List<StreamDetail> ns2Streams = streamClient.list(NS2);
    Assert.assertEquals(1, ns2Streams.size());
    Assert.assertEquals(streamName, ns2Streams.get(0).getName());

    // Sending events into the stream in namespace1 allows that event to be fetched afterwards.
    streamClient.sendEvent(streamId1, "testEvent");
    List<StreamEvent> events1 = streamClient.getEvents(streamId1, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                        Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(1, events1.size());
    Assert.assertEquals("testEvent", Bytes.toString(events1.get(0).getBody()));

    // The set of events is different for streams in separate namespaces, even though the names are the same.
    List<StreamEvent> events2 = streamClient.getEvents(streamId2, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                        Lists.<StreamEvent>newArrayList());
    Assert.assertTrue(events2.isEmpty());
  }
}
