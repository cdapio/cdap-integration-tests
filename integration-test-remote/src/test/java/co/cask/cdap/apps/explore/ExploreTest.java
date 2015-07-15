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

package co.cask.cdap.apps.explore;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicMarkableReference;
import javax.annotation.Nullable;

/**
 * Tests Explore functionality using {@link WordCountApplication}.
 */
public class ExploreTest extends AudiTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ExploreTest.class);

  @Test
  public void test() throws Exception {
    ApplicationManager app = deployApplication(WordCountApplication.class);
    FlowManager wordCountFlow = app.getFlowManager("WordCountFlow").start();
    FlowManager extendedWordCountFlow = app.getFlowManager("ExtendedWordCountFlow").start();
    FlowManager keyValueFlow = app.getFlowManager("KeyValueFlow").start();
    ServiceManager wordCountService = app.getServiceManager("WordCountService").start();
    waitForStatus(true, wordCountFlow, extendedWordCountFlow, keyValueFlow, wordCountService);

    sendInputData();
    testEqualityJoin();
    testLeftOuterJoin();
    testRightOuterJoin();
    testFullOuterJoin();
    testCountEqualityJoin();
    testSumLeftOuterJoin();
    testAvgLeftOuterJoin();
    testMinRightOuterJoin();
    testMaxRightOuterJoin();
  }

  private void sendInputData() throws Exception {
    Id.Stream listsStreamId = Id.Stream.from(Id.Namespace.DEFAULT, "lists");
    Id.Stream wordsStreamId = Id.Stream.from(Id.Namespace.DEFAULT, "words");
    Id.Stream words2StreamId = Id.Stream.from(Id.Namespace.DEFAULT, "words2");

    StreamClient streamClient = getStreamClient();
    streamClient.sendEvent(listsStreamId, "Mike 12 32 0");
    streamClient.sendEvent(listsStreamId, "iPad 902 332 2286");
    streamClient.sendEvent(listsStreamId, "Jada");
    streamClient.sendEvent(listsStreamId, "Spike 8023 334 0 34");
    streamClient.sendEvent(wordsStreamId, "Mike has macbook.");
    streamClient.sendEvent(wordsStreamId, "Mike has iPad.");
    streamClient.sendEvent(wordsStreamId, "Jada has iPad.");
    streamClient.sendEvent(words2StreamId, "foo bar foo foobar barbar foobarbar");

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "WordCountApplication");
    Id.Flow keyValueFlowId = Id.Flow.from(appId, "KeyValueFlow");

    // verify processed count

    MetricsClient metricsClient = getMetricsClient();
    final RuntimeMetrics wordSplitterMetrics = metricsClient.getFlowletMetrics(keyValueFlowId, "wordSplitter");
    assertWithRetry(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Assert.assertEquals(4, wordSplitterMetrics.getProcessed());
        return null;
      }
    }, 15, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    Id.Flow wordCountFlowId = Id.Flow.from(appId, "WordCountFlow");
    final RuntimeMetrics wordCounterMetrics = metricsClient.getFlowletMetrics(wordCountFlowId, "wordCounter");
    assertWithRetry(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Assert.assertEquals(3, wordCounterMetrics.getProcessed());
        return null;
      }
    }, 15, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    Id.Flow extWordCountFlowId = Id.Flow.from(appId, "ExtendedWordCountFlow");
    final RuntimeMetrics extWordCounterMetrics = metricsClient.getFlowletMetrics(extWordCountFlowId, "wordCounter");
    assertWithRetry(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Assert.assertEquals(1, extWordCounterMetrics.getProcessed());
        return null;
      }
    }, 15, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    // verify stream content
    assertStreamEvents(listsStreamId, "Mike 12 32 0", "iPad 902 332 2286", "Jada", "Spike 8023 334 0 34");
    assertStreamEvents(wordsStreamId, "Mike has macbook.", "Mike has iPad.", "Jada has iPad.");
    assertStreamEvents(words2StreamId, "foo bar foo foobar barbar foobarbar");
  }

  private void testEqualityJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select t1.word,t1.count,t2.key,t2.value "
        + "from dataset_wordcounts t1 "
        + "join dataset_kvtable t2 "
        + "on (t1.word = t2.key) "
        + "order by t1.word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(3, rows.size());

    Assert.assertEquals("Jada", rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));
    Assert.assertEquals("Jada", rows.get(0).get(2));
    Assert.assertEquals("Mike", rows.get(1).get(0));
    Assert.assertEquals(2L, rows.get(1).get(1));
    Assert.assertEquals("Mike", rows.get(1).get(2));
    Assert.assertEquals("iPad", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));
    Assert.assertEquals("iPad", rows.get(2).get(2));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
    Assert.assertEquals("STRING", resultSchema.get(2).getType());
    Assert.assertEquals(3, resultSchema.get(2).getPosition());
    Assert.assertEquals("struct<name:string,ints:array<int>>", resultSchema.get(3).getType());
    Assert.assertEquals(4, resultSchema.get(3).getPosition());
  }

  private void testLeftOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select t1.word,t1.count,t2.key,t2.value from dataset_wordcounts t1 "
        + "LEFT OUTER JOIN dataset_kvtable t2 "
        + "ON (t1.word = t2.key) order by t1.word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(5, rows.size());

    Assert.assertEquals("Jada", rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));
    Assert.assertEquals("Jada", rows.get(0).get(2));
    Assert.assertEquals("Mike", rows.get(1).get(0));
    Assert.assertEquals(2L, rows.get(1).get(1));
    Assert.assertEquals("Mike", rows.get(1).get(2));

    Assert.assertEquals("has", rows.get(2).get(0));
    Assert.assertEquals(3L, rows.get(2).get(1));
    Assert.assertEquals(null, rows.get(2).get(2));

    Assert.assertEquals("iPad", rows.get(3).get(0));
    Assert.assertEquals(2L, rows.get(3).get(1));
    Assert.assertEquals("iPad", rows.get(3).get(2));

    Assert.assertEquals("macbook", rows.get(4).get(0));
    Assert.assertEquals(1L, rows.get(4).get(1));
    Assert.assertEquals(null, rows.get(4).get(2));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
    Assert.assertEquals("STRING", resultSchema.get(2).getType());
    Assert.assertEquals(3, resultSchema.get(2).getPosition());
    Assert.assertEquals("struct<name:string,ints:array<int>>", resultSchema.get(3).getType());
    Assert.assertEquals(4, resultSchema.get(3).getPosition());
  }

  private void testRightOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select t1.word,t1.count,t2.key,t2.value from dataset_wordcounts t1 "
        + "RIGHT OUTER JOIN dataset_kvtable t2 "
        + "ON (t1.word = t2.key) order by t1.word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(4, rows.size());

    Assert.assertEquals(null, rows.get(0).get(0));
    Assert.assertEquals("Spike", rows.get(0).get(2));

    Assert.assertEquals("Jada", rows.get(1).get(0));
    Assert.assertEquals(1L, rows.get(1).get(1));
    Assert.assertEquals("Jada", rows.get(1).get(2));

    Assert.assertEquals("Mike", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));
    Assert.assertEquals("Mike", rows.get(2).get(2));

    Assert.assertEquals("iPad", rows.get(3).get(0));
    Assert.assertEquals(2L, rows.get(3).get(1));
    Assert.assertEquals("iPad", rows.get(3).get(2));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
    Assert.assertEquals("STRING", resultSchema.get(2).getType());
    Assert.assertEquals(3, resultSchema.get(2).getPosition());
    Assert.assertEquals("struct<name:string,ints:array<int>>", resultSchema.get(3).getType());
    Assert.assertEquals(4, resultSchema.get(3).getPosition());
  }

  private void testFullOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select t1.word,t1.count,t2.key,t2.value from dataset_wordcounts t1 "
        + "FULL OUTER JOIN dataset_kvtable t2 "
        + "ON (t1.word = t2.key) order by t1.word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(6, rows.size());

    Assert.assertEquals(null, rows.get(0).get(0));
    Assert.assertEquals("Spike", rows.get(0).get(2));

    Assert.assertEquals("Jada", rows.get(1).get(0));
    Assert.assertEquals(1L, rows.get(1).get(1));
    Assert.assertEquals("Jada", rows.get(1).get(2));

    Assert.assertEquals("Mike", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));
    Assert.assertEquals("Mike", rows.get(2).get(2));

    Assert.assertEquals("has", rows.get(3).get(0));
    Assert.assertEquals(3L, rows.get(3).get(1));
    Assert.assertEquals(null, rows.get(3).get(2));

    Assert.assertEquals("iPad", rows.get(4).get(0));
    Assert.assertEquals(2L, rows.get(4).get(1));
    Assert.assertEquals("iPad", rows.get(4).get(2));

    Assert.assertEquals("macbook", rows.get(5).get(0));
    Assert.assertEquals(1L, rows.get(5).get(1));
    Assert.assertEquals(null, rows.get(5).get(2));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
    Assert.assertEquals("STRING", resultSchema.get(2).getType());
    Assert.assertEquals(3, resultSchema.get(2).getPosition());
    Assert.assertEquals("struct<name:string,ints:array<int>>", resultSchema.get(3).getType());
    Assert.assertEquals(4, resultSchema.get(3).getPosition());
  }

  private void testCountEqualityJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select t1.count,count(*) as rowCount from dataset_wordcounts t1 "
        + "JOIN  dataset_kvtable t2 ON (t1.word = t2.key) group by t1.count").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(2, rows.size());

    Assert.assertEquals(1L, rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));

    Assert.assertEquals(2L, rows.get(1).get(0));
    Assert.assertEquals(2L, rows.get(1).get(1));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(2, resultSchema.size());
    Assert.assertEquals("BIGINT", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
  }

  private void testSumLeftOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select sum(t1.count) as sum from dataset_wordcounts t1 "
        + "LEFT OUTER JOIN dataset_kvtable t2 ON (t1.word = t2.key)").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(1, rows.size());

    Assert.assertEquals(9L, rows.get(0).get(0));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(1, resultSchema.size());
    Assert.assertEquals("sum", resultSchema.get(0).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
  }

  private void testAvgLeftOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select avg(t1.count) as avg from dataset_wordcounts t1 "
        + "LEFT OUTER JOIN dataset_kvtable t2 ON (t1.word = t2.key)").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(1, rows.size());

    Assert.assertEquals(1.8, rows.get(0).get(0));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(1, resultSchema.size());
    Assert.assertEquals("avg", resultSchema.get(0).getName());
    Assert.assertEquals("DOUBLE", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
  }

  private void testMinRightOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select min(t1.count) as min from dataset_wordcounts t1 "
        + "RIGHT OUTER JOIN dataset_kvtable t2 ON (t1.word = t2.key)").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(1, rows.size());

    Assert.assertEquals(1L, rows.get(0).get(0));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(1, resultSchema.size());
    Assert.assertEquals("min", resultSchema.get(0).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
  }

  private void testMaxRightOuterJoin() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select max(t1.count) as max from dataset_wordcounts t1 "
        + "RIGHT OUTER JOIN dataset_kvtable t2 ON (t1.word = t2.key)").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(1, rows.size());

    Assert.assertEquals(2L, rows.get(0).get(0));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(1, resultSchema.size());
    Assert.assertEquals("max", resultSchema.get(0).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());
  }

  private List<List<Object>> executionResult2Rows(ExploreExecutionResult executionResult) {
    List<List<Object>> rows = Lists.newArrayList();
    while (executionResult.hasNext()) {
      rows.add(executionResult.next().getColumns());
    }
    return rows;
  }

  private void assertStreamEvents(Id.Stream streamId, String... expectedEvents)
    throws UnauthorizedException, IOException, StreamNotFoundException {

    List<StreamEvent> streamEvents = Lists.newArrayList();
    getStreamClient().getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, streamEvents);
    List<String> streamEventsAsStrings = Lists.transform(streamEvents, new Function<StreamEvent, String>() {
      @Nullable
      @Override
      public String apply(StreamEvent input) {
        return Bytes.toString(input.getBody());
      }
    });
    Assert.assertArrayEquals(expectedEvents, streamEventsAsStrings.toArray(new String[streamEventsAsStrings.size()]));
  }

  private void waitForStatus(boolean status, ProgramManager... managers) throws InterruptedException {
    for (ProgramManager manager : managers) {
      manager.waitForStatus(status);
    }
  }

  private <T> T assertWithRetry(final Callable<T> callable, long timeout, TimeUnit timeoutUnit,
                                long sleepDelay, TimeUnit sleepDelayUnit)
    throws InterruptedException, ExecutionException, TimeoutException {

    final AtomicMarkableReference<T> result = new AtomicMarkableReference<>(null, false);
      Tasks.waitFor(true, new Callable<Boolean>() {
        public Boolean call() throws Exception {
          try {
            result.set(callable.call(), true);
          } catch (AssertionError e) {
            LOG.warn("Assertion failed", e);
            // retry
            return false;
          }
          return true;
        }
      }, timeout, timeoutUnit, sleepDelay, sleepDelayUnit);
    Assert.assertTrue(result.isMarked());
    return result.getReference();
  }
}
