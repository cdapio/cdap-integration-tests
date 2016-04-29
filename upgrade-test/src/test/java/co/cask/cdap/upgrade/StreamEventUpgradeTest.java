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

package co.cask.cdap.upgrade;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamDetail;
import com.google.common.collect.Lists;
import org.junit.Assert;

import java.util.List;

/**
 * Tests that stream events ingested before an upgrade can be retrieved after an upgrade.
 */
public class StreamEventUpgradeTest extends UpgradeTestBase {
  private static final Id.Stream FOO_STREAM = Id.Stream.from(TEST_NAMESPACE, "fooStream");
  private static final int MSG_COUNT = 50;

  @Override
  protected void preStage() throws Exception {
    StreamClient streamClient = getStreamClient();
    List<String> initialStreams = toStringList(streamClient.list(TEST_NAMESPACE));
    Assert.assertFalse(String.format("Stream %s was expected not to exist. Stream list: %s",
                                     FOO_STREAM, initialStreams),
                       initialStreams.contains(FOO_STREAM.getId()));
    streamClient.create(FOO_STREAM);

    // Ingest MSG_COUNT events to the stream
    for (int i = 0; i < MSG_COUNT; i++) {
      streamClient.sendEvent(FOO_STREAM, "event" + i);
    }
  }

  @Override
  protected void postStage() throws Exception {
    StreamClient streamClient = getStreamClient();
    List<String> streams = toStringList(streamClient.list(TEST_NAMESPACE));
    Assert.assertTrue(String.format("Stream %s was expected to exist. Stream list: %s",
                                    FOO_STREAM, streams),
                      streams.contains(FOO_STREAM.getId()));

    // Verify that events fetched from the stream are the same as the ones sent in
    List<StreamEvent> events =
      streamClient.getEvents(FOO_STREAM, 0, Long.MAX_VALUE, Integer.MAX_VALUE, Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(MSG_COUNT, events.size());
    for (int i = 0; i < MSG_COUNT; i++) {
      Assert.assertEquals("event" + i, Bytes.toString(events.get(i).getBody()));
    }
  }

  // necessary because StreamRecord doesn't have a useful toString
  private List<String> toStringList(List<StreamDetail> streamRecords) {
    List<String> streamNames = Lists.newArrayList();
    for (StreamDetail streamRecord : streamRecords) {
      streamNames.add(streamRecord.getName());
    }
    return streamNames;
  }
}
