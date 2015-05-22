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
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.StreamNotFoundException;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.StreamProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StreamTest extends AudiTestBase {

  // TODO: If it's better that way, we can split this test case into two: invalid interactions and valid interactions
  @Test
  public void testStreams() throws Exception {
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    // test interaction with nonexistent stream; should fail
    String nonexistentStream = "nonexistentStream";
    try {
      streamClient.getConfig(nonexistentStream);
      Assert.fail(String.format("Expected '%s' to not exist.", nonexistentStream));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.sendEvent(nonexistentStream, "testEvent");
      Assert.fail(String.format("Expected '%s' to not exist.", nonexistentStream));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.getEvents(nonexistentStream, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                             Lists.<StreamEvent>newArrayList());
      Assert.fail(String.format("Expected '%s' to not exist.", nonexistentStream));
    } catch (StreamNotFoundException expected) {
    }
    try {
      streamClient.truncate(nonexistentStream);
      Assert.fail(String.format("Expected '%s' to not exist.", nonexistentStream));
    } catch (StreamNotFoundException expected) {
    }

    // creation with invalid characters should fail
    String streamName = "testStream";
    try {
      streamClient.create(streamName + "&");
      Assert.fail();
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(
        "Stream name can only contain alphanumeric, '-' and '_' characters"));
    }
    try {
      streamClient.create(streamName + ".");
      Assert.fail();
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(
        "Stream name can only contain alphanumeric, '-' and '_' characters"));
    }

    // test valid scenarios
    streamClient.create(streamName);
    StreamProperties config = streamClient.getConfig(streamName);
    Assert.assertNotNull(config);

    // create is idempotent
    streamClient.create(streamName);

    streamClient.sendEvent(streamName, "");
    streamClient.sendEvent(streamName, " a b ");
    ArrayList<StreamEvent> events = streamClient.getEvents(streamName, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                           Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(2, events.size());
    Assert.assertEquals("", Bytes.toString(events.get(0).getBody()));
    Assert.assertEquals(" a b ", Bytes.toString(events.get(1).getBody()));

    MetricsClient metricsClient = new MetricsClient(getClientConfig(), getRestClient());


    ImmutableMap<String, String> streamTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
                                                              Constants.Metrics.Tag.STREAM, streamName);
    checkEventsProcessed(metricsClient, streamTags, "system.collect.events", 2, 10);
    checkEventsProcessed(metricsClient, streamTags, "system.collect.bytes", 5, 10);

    streamClient.truncate(streamName);
    events = streamClient.getEvents(streamName, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                    Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(0, events.size());
  }

  private void checkEventsProcessed(MetricsClient metricsClient, Map<String, String> streamTags, String metric,
                                    long expectedCount, int retries) throws Exception {
    for (int i = 0; i < retries; i++) {
      long numProcessed = getNumProcessed(metricsClient, streamTags, metric);
      if (numProcessed == expectedCount) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(expectedCount, getNumProcessed(metricsClient, streamTags, metric));
  }


  private long getNumProcessed(MetricsClient metricsClient, Map<String, String> streamTags,
                               String metric) throws Exception {
    MetricQueryResult metricQueryResult = metricsClient.query(metric, streamTags);
    MetricQueryResult.TimeSeries[] series = metricQueryResult.getSeries();
    if (series.length == 0) {
      return 0;
    }
    return series[0].getData()[0].getValue();
  }
}
