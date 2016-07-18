/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.longrunning.logmapreduce;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.longrunning.datacleansing.DataCleansingApp;
import co.cask.cdap.longrunning.datacleansing.DataCleansingTestState;
import co.cask.cdap.longrunning.datacleansing.Person;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Data Cleansing long running test
 */
public class LogMapReduceTest extends LongRunningTestBase<LogMapReduceTestState> {
  private static final Logger LOG = LoggerFactory.getLogger(LogMapReduceTest.class);

  private static final int BATCH_SIZE = 100;
  private static final String LOG_MAPREDUCE_NAME = "LogMap";
  private static final Gson GSON = new Gson();

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), LogMapReduceApp.class);
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
  }

  @Override
  public LogMapReduceTestState getInitialState() {
    return new LogMapReduceTestState(0, 0);
  }

  @Override
  public void awaitOperations(LogMapReduceTestState state) throws Exception {
    getApplicationManager().getMapReduceManager(LOG_MAPREDUCE_NAME).waitForFinish(5, TimeUnit.MINUTES);
  }

  @Override
  public void verifyRuns(LogMapReduceTestState state) throws Exception {
    LOG.info("verifying runs for logs");
    // For now, check total number of clean records and invalid records
//    Assert.assertEquals(state.getRunId(), getTotalRecords(true) + getTotalRecords(false));

    // verify segregated records
//    Assert.assertTrue(verifyRecordsWithExplore(state));
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().toEntityId().app(LogMapReduceApp.NAME));
  }

  @Override
  public LogMapReduceTestState runOperations(LogMapReduceTestState state) throws Exception {
    StreamClient streamClient = getStreamClient();
    LOG.info("Writing {} events in one batch", BATCH_SIZE);
    StringWriter writer = new StringWriter();
    for (int i = 0; i < BATCH_SIZE; i++) {
      writer.write(String.format("%010d", i));
      writer.write("\n");
    }
    streamClient.sendBatch(Id.Stream.from(getLongRunningNamespace(), LogMapReduceApp.EVENTS_STREAM), "text/plain",
                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));

    // run the mapreduce
    final long startTime = System.currentTimeMillis() + 1;
    MapReduceManager mapReduceManager = getApplicationManager().getMapReduceManager("LogMap")
      .start(ImmutableMap.of("logical.start.time", Long.toString(startTime)));
    mapReduceManager.waitForFinish(1, TimeUnit.MINUTES);

    long now = System.currentTimeMillis();
    return new LogMapReduceTestState(now, state.getRunId() + 1);
  }

//  private void createPartition(URL serviceUrl, DataCleansingTestState state)
//    throws IOException, UnauthenticatedException {
//    URL url = new URL(serviceUrl, "v1/records/raw");
//    List<String> records = new ArrayList<>();
//    generateRecords(records, state.getEndInvalidRecordPid() + 1, false);
//    generateRecords(records, state.getEndInvalidRecordPid() + CLEAN_RECORDS_PER_BATCH + 1, true);
//    String body = Joiner.on("\n").join(records) + "\n";
//    HttpRequest request = HttpRequest.post(url).withBody(body).build();
//    HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
//    Assert.assertEquals(200, response.getResponseCode());
//  }

//  private void generateRecords(List<String> records, long start, boolean invalid) {
//    long numRecords = invalid ? INVALID_RECORDS_PER_BATCH : CLEAN_RECORDS_PER_BATCH;
//    for (long i = start; i < (start + numRecords); i++) {
//      records.add(getRecord(i, invalid));
//    }
//  }

  private String getRecord(long index, boolean invalid) {
    String zip = invalid ? "84125q" : "84125";
    return GSON.toJson(new Person(index, "bob", "02-12-1983", zip));
  }

  // pass true to get the number of invalid records; pass false to get the number of valid records processed.
  private long getTotalRecords(boolean invalid) throws Exception {
    DatasetId totalRecordsTableId = new DatasetId(getLongRunningNamespace().getId(),
                                                  DataCleansingApp.TOTAL_RECORDS_TABLE);
    KeyValueTable totalRecordsTable = getKVTableDataset(totalRecordsTableId).get();
    byte[] recordKey = invalid ? DataCleansingApp.INVALID_RECORD_KEY : DataCleansingApp.CLEAN_RECORD_KEY;
    return readLong(totalRecordsTable.read(recordKey));
  }

  // TODO: Use serivce instead of explore as Explore is slower
  private boolean verifyRecordsWithExplore(DataCleansingTestState state) throws Exception {
//    QueryClient queryClient = new QueryClient(getClientConfig());
//    String cleanRecordsQuery = "SELECT * FROM dataset_" + CLEAN_RECORDS_DATASET + " where TIME = "
//      + state.getTimestamp();
//    String invalidRecordsQuery = "SELECT * FROM dataset_" + INVALID_RECORDS_DATASET + " where TIME = "
//      + state.getTimestamp();
//
//    // Reduce wait time by submitting both the queries
//    ListenableFuture<ExploreExecutionResult> cleanRecordsExecute = queryClient.execute(getLongRunningNamespace(),
//                                                                                       cleanRecordsQuery);
//    ListenableFuture<ExploreExecutionResult> invalidRecordsExecute = queryClient.execute(getLongRunningNamespace(),
//                                                                                         invalidRecordsQuery);
//    ExploreExecutionResult cleanRecordsResult = cleanRecordsExecute.get();
//    ExploreExecutionResult invalidRecordsResult = invalidRecordsExecute.get();
//
//    return (verifyResults(cleanRecordsResult, state.getStartCleanRecordPid(), false) &&
//      verifyResults(invalidRecordsResult, state.getStartInvalidRecordPid(), true));
    return false;
  }

  private boolean verifyResults(ExploreExecutionResult result, long index, boolean invalid) {
    while (result.hasNext()) {
      QueryResult next = result.next();
      List<Object> columns = next.getColumns();
      String expectedRecord = getRecord(index, invalid);
      if (!expectedRecord.equalsIgnoreCase((String) columns.get(0))) {
        return false;
      }
      index++;
    }
    return true;
  }

  private long readLong(byte[] bytes) {
    return bytes == null ? 0 : Bytes.toLong(bytes);
  }
}
