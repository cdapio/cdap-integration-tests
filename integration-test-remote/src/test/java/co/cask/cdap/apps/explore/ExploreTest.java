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
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests Explore functionality using {@link WordCountApplication}.
 */
public class ExploreTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreTest.class);
  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    LOG.info("Sending input data.");
    sendInputData();

    LOG.info("Testing equality join.");
    testEqualityJoin();
    LOG.info("Testing left outer join.");
    testLeftOuterJoin();
    LOG.info("Testing right outer join.");
    testRightOuterJoin();
    LOG.info("Testing full outer join.");
    testFullOuterJoin();
    LOG.info("Testing count equality join.");
    testCountEqualityJoin();
    LOG.info("Testing sum left outer join.");
    testSumLeftOuterJoin();
    LOG.info("Testing avg left outer join.");
    testAvgLeftOuterJoin();
    LOG.info("Testing min right outer join.");
    testMinRightOuterJoin();
    LOG.info("Testing max right outer join.");
    testMaxRightOuterJoin();

    LOG.info("Testing check KV Table.");
    testCheckKVTable();
    LOG.info("Testing stream query.");
    testStreamQuery();
    // TODO: Move to the beginning when https://issues.cask.co/browse/CDAP-3757 is fixed
    LOG.info("Testing insert query.");
    testInsertQuery();
  }

  private void testInsertQuery() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());

    // Query: insert into other dataset, different type
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "insert into table dataset_wordcounts2 select word,count from dataset_extendedwordcounts").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    results = client.execute(
      Id.Namespace.DEFAULT,
      "select * from dataset_wordcounts2 order by word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(5, rows.size());

    Assert.assertEquals("bar", rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));
    Assert.assertEquals("barbar", rows.get(1).get(0));
    Assert.assertEquals(1L, rows.get(1).get(1));
    Assert.assertEquals("foo", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));
    Assert.assertEquals("foobar", rows.get(3).get(0));
    Assert.assertEquals(1L, rows.get(3).get(1));
    Assert.assertEquals("foobarbar", rows.get(4).get(0));
    Assert.assertEquals(1L, rows.get(4).get(1));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(2, resultSchema.size());

    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());

    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());


    // Query: insert into other dataset, same type
    results = client.execute(
      Id.Namespace.DEFAULT,
      "insert into table dataset_wordcounts2 select * from dataset_wordcounts").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    results = client.execute(
      Id.Namespace.DEFAULT,
      "select * from dataset_wordcounts2 order by word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    rows = executionResult2Rows(results);
    Assert.assertEquals(10, rows.size());

    Assert.assertEquals("Jada", rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));
    Assert.assertEquals("Mike", rows.get(1).get(0));
    Assert.assertEquals(2L, rows.get(1).get(1));
    Assert.assertEquals("bar", rows.get(2).get(0));
    Assert.assertEquals(1L, rows.get(2).get(1));
    Assert.assertEquals("barbar", rows.get(3).get(0));
    Assert.assertEquals(1L, rows.get(3).get(1));
    Assert.assertEquals("foo", rows.get(4).get(0));
    Assert.assertEquals(2L, rows.get(4).get(1));
    Assert.assertEquals("foobar", rows.get(5).get(0));
    Assert.assertEquals(1L, rows.get(5).get(1));
    Assert.assertEquals("foobarbar", rows.get(6).get(0));
    Assert.assertEquals(1L, rows.get(6).get(1));
    Assert.assertEquals("has", rows.get(7).get(0));
    Assert.assertEquals(3L, rows.get(7).get(1));
    Assert.assertEquals("iPad", rows.get(8).get(0));
    Assert.assertEquals(2L, rows.get(8).get(1));
    Assert.assertEquals("macbook", rows.get(9).get(0));
    Assert.assertEquals(1L, rows.get(9).get(1));


    // insert into cdap_user_wordcounts from itself
    results = client.execute(
      Id.Namespace.DEFAULT,
      "insert into table dataset_wordcounts2 select * from dataset_wordcounts2").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    results = client.execute(
      Id.Namespace.DEFAULT,
      "select * from dataset_wordcounts2 order by word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    rows = executionResult2Rows(results);
    Assert.assertEquals(10, rows.size());

    // NOTE: because of the nature of the wordCounts dataset,
    // writing to itself will double the values of the existing keys
    Assert.assertEquals("Jada", rows.get(0).get(0));
    Assert.assertEquals(2L, rows.get(0).get(1));
    Assert.assertEquals("Mike", rows.get(1).get(0));
    Assert.assertEquals(4L, rows.get(1).get(1));
    Assert.assertEquals("bar", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));
    Assert.assertEquals("barbar", rows.get(3).get(0));
    Assert.assertEquals(2L, rows.get(3).get(1));
    Assert.assertEquals("foo", rows.get(4).get(0));
    Assert.assertEquals(4L, rows.get(4).get(1));
    Assert.assertEquals("foobar", rows.get(5).get(0));
    Assert.assertEquals(2L, rows.get(5).get(1));
    Assert.assertEquals("foobarbar", rows.get(6).get(0));
    Assert.assertEquals(2L, rows.get(6).get(1));
    Assert.assertEquals("has", rows.get(7).get(0));
    Assert.assertEquals(6L, rows.get(7).get(1));
    Assert.assertEquals("iPad", rows.get(8).get(0));
    Assert.assertEquals(4L, rows.get(8).get(1));
    Assert.assertEquals("macbook", rows.get(9).get(0));
    Assert.assertEquals(2L, rows.get(9).get(1));
  }

  private void sendInputData() throws Exception {
    ApplicationManager app = deployApplication(WordCountApplication.class);
    FlowManager wordCountFlow = app.getFlowManager("WordCountFlow").start();
    FlowManager extendedWordCountFlow = app.getFlowManager("ExtendedWordCountFlow").start();
    FlowManager keyValueFlow = app.getFlowManager("KeyValueFlow").start();
    ServiceManager wordCountService = app.getServiceManager("WordCountService").start();
    waitForStatus(true, wordCountFlow, extendedWordCountFlow, keyValueFlow, wordCountService);

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

    // verify stream content
    assertStreamEvents(listsStreamId, "Mike 12 32 0", "iPad 902 332 2286", "Jada", "Spike 8023 334 0 34");
    assertStreamEvents(wordsStreamId, "Mike has macbook.", "Mike has iPad.", "Jada has iPad.");
    assertStreamEvents(words2StreamId, "foo bar foo foobar barbar foobarbar");

    // verify processed count
    keyValueFlow.getFlowletMetrics("wordSplitter").waitForProcessed(4, 2, TimeUnit.MINUTES);
    wordCountFlow.getFlowletMetrics("wordCounter").waitForProcessed(3, 2, TimeUnit.MINUTES);
    extendedWordCountFlow.getFlowletMetrics("wordCounter").waitForProcessed(1, 2, TimeUnit.MINUTES);

    wordCountFlow.stop();
    extendedWordCountFlow.stop();
    keyValueFlow.stop();
    wordCountService.stop();
    waitForStatus(false, wordCountFlow, extendedWordCountFlow, keyValueFlow, wordCountService);
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

  private void testCheckKVTable() throws Exception {
    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select * from dataset_kvtable order by key").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(4, rows.size());

    Assert.assertEquals("Jada", rows.get(0).get(0));
    JsonObject json = GSON.fromJson((String) rows.get(0).get(1), JsonObject.class);
    Assert.assertEquals("Jada", json.get("name").getAsString());
    Assert.assertEquals("[]", GSON.toJson(json.get("ints").getAsJsonArray()));

    Assert.assertEquals("Mike", rows.get(1).get(0));
    json = GSON.fromJson((String) rows.get(1).get(1), JsonObject.class);
    Assert.assertEquals("Mike", json.get("name").getAsString());
    Assert.assertEquals("[12,32,0]", GSON.toJson(json.get("ints").getAsJsonArray()));

    Assert.assertEquals("Spike", rows.get(2).get(0));
    json = GSON.fromJson((String) rows.get(2).get(1), JsonObject.class);
    Assert.assertEquals("Spike", json.get("name").getAsString());
    Assert.assertEquals("[8023,334,0,34]", GSON.toJson(json.get("ints").getAsJsonArray()));

    Assert.assertEquals("iPad", rows.get(3).get(0));
    json = GSON.fromJson((String) rows.get(3).get(1), JsonObject.class);
    Assert.assertEquals("iPad", json.get("name").getAsString());
    Assert.assertEquals("[902,332,2286]", GSON.toJson(json.get("ints").getAsJsonArray()));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(2, resultSchema.size());

    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());

    Assert.assertEquals("struct<name:string,ints:array<int>>", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
  }

  @Test
  @Category({HDP20Incompatible.class, CDH51Incompatible.class})
  public void testSubqueryInWhereClause() throws Exception {
    // Have to send input data all over again, because IntegrationTestBase methods are not static, so cannot be
    // run in @BeforeClass.
    LOG.info("Sending input data.");
    sendInputData();

    LOG.info("Testing subquery in where clause.");
    QueryClient client = new QueryClient(getClientConfig());
    String subquery = "select key from dataset_kvtable t2 where t1.word = t2.key";
    ExploreExecutionResult results = client.execute(
      Id.Namespace.DEFAULT,
      "select * from dataset_wordcounts t1 where exists (" + subquery + ") order by t1.word").get();

    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(3, rows.size());

    Assert.assertEquals("Jada", rows.get(0).get(0));
    Assert.assertEquals(1L, rows.get(0).get(1));
    Assert.assertEquals("Mike", rows.get(1).get(0));
    Assert.assertEquals(2L, rows.get(1).get(1));
    Assert.assertEquals("iPad", rows.get(2).get(0));
    Assert.assertEquals(2L, rows.get(2).get(1));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(2, resultSchema.size());

    Assert.assertEquals("t1.word", resultSchema.get(0).getName());
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());

    Assert.assertEquals("t1.count", resultSchema.get(1).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());
  }

  private void testStreamQuery() throws Exception {
    StreamClient streamClient = getStreamClient();

    // send stream events for stream explore testing
    Id.Stream tradesStreamId = Id.Stream.from(Id.Namespace.DEFAULT, "trades");
    streamClient.create(tradesStreamId);
    streamClient.sendEvent(tradesStreamId, "AAPL,50,112.98");
    streamClient.sendEvent(tradesStreamId, "AAPL,100,112.87");
    streamClient.sendEvent(tradesStreamId, "AAPL,8,113.02");
    streamClient.sendEvent(tradesStreamId, "NFLX,10,437.45");

    QueryClient client = new QueryClient(getClientConfig());
    ExploreExecutionResult results = client.execute(Id.Namespace.DEFAULT, "select * from stream_trades").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    Assert.assertEquals(4, rows.size());

    Assert.assertEquals("AAPL,50,112.98", rows.get(0).get(2));
    Assert.assertEquals("AAPL,100,112.87", rows.get(1).get(2));
    Assert.assertEquals("AAPL,8,113.02", rows.get(2).get(2));
    Assert.assertEquals("NFLX,10,437.45", rows.get(3).get(2));

    // set stream format trades csv "ticker string, num_traded int, price double"
    FormatSpecification formatSpecification = new FormatSpecification(
      "csv", Schema.parseSQL("ticker string, num_traded int, price double"), ImmutableMap.<String, String>of());

    StreamProperties currentProperties = streamClient.getConfig(tradesStreamId);
    StreamProperties streamProperties = new StreamProperties(currentProperties.getTTL(),
                                                             formatSpecification,
                                                             currentProperties.getNotificationThresholdMB());
    streamClient.setStreamProperties(tradesStreamId, streamProperties);

    results = client.execute(
      Id.Namespace.DEFAULT,
      "select ticker, count(*) as transactions, sum(num_traded) as volume from stream_trades " +
        "group by ticker order by volume desc").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    rows = executionResult2Rows(results);
    Assert.assertEquals(2, rows.size());

    Assert.assertEquals("AAPL", rows.get(0).get(0));
    Assert.assertEquals(3L, rows.get(0).get(1));
    Assert.assertEquals(158L, rows.get(0).get(2));
    Assert.assertEquals("NFLX", rows.get(1).get(0));
    Assert.assertEquals(1L, rows.get(1).get(1));
    Assert.assertEquals(10L, rows.get(1).get(2));

    List<ColumnDesc> resultSchema = results.getResultSchema();
    Assert.assertEquals(3, resultSchema.size());

    Assert.assertEquals("ticker", resultSchema.get(0).getName());
    Assert.assertEquals("STRING", resultSchema.get(0).getType());
    Assert.assertEquals(1, resultSchema.get(0).getPosition());

    Assert.assertEquals("transactions", resultSchema.get(1).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(1).getType());
    Assert.assertEquals(2, resultSchema.get(1).getPosition());

    Assert.assertEquals("volume", resultSchema.get(2).getName());
    Assert.assertEquals("BIGINT", resultSchema.get(2).getType());
    Assert.assertEquals(3, resultSchema.get(2).getPosition());
  }

  private List<List<Object>> executionResult2Rows(ExploreExecutionResult executionResult) {
    List<List<Object>> rows = Lists.newArrayList();
    while (executionResult.hasNext()) {
      rows.add(executionResult.next().getColumns());
    }
    return rows;
  }

  private void assertStreamEvents(Id.Stream streamId, String... expectedEvents)
    throws UnauthenticatedException, IOException, StreamNotFoundException {

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
      manager.waitForStatus(status, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    }
  }

}
