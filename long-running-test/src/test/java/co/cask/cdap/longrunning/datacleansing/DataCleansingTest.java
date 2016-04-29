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

package co.cask.cdap.longrunning.datacleansing;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.datacleansing.DataCleansingService;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import org.junit.Assert;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Data Cleansing long running test
 */
public class DataCleansingTest extends LongRunningTestBase<DataCleansingTestState> {
  private static final int CLEAN_RECORDS_PER_BATCH = 70;
  private static final int INVALID_RECORDS_PER_BATCH = 30;

  private static final String DATACLEANSING_MAPREDUCE_NAME = "DataCleansingMapReduce";
  private static final String DATACLEANSING_NAME = "DataCleansing";
  private static final String OUTPUT_PARTITION_KEY = "output.partition.key";
  private static final String SCHEMA_KEY = "schema.key";
  private static final String SCHEMAJSON = DataCleansingMapReduce.SchemaMatchingFilter.DEFAULT_SCHEMA.toString();

  private static final String CLEAN_RECORDS_DATASET = "cleanRecords";
  private static final String INVALID_RECORDS_DATASET = "invalidRecords";

  private static final Gson GSON = new Gson();

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), DataCleansingApp.class);
  }

  @Override
  public void start() throws Exception {
    ServiceManager serviceManager = getApplicationManager().getServiceManager(DataCleansingService.NAME).start();
    serviceManager.waitForStatus(true);
    URL serviceURL = serviceManager.getServiceURL();
    // until the service starts, the endpoint will return 503. Once it's up, it'll return 404
    retryRestCalls(HttpURLConnection.HTTP_NOT_FOUND,
                   HttpRequest.get(new URL(serviceURL, "nonExistentEndpoint")).build());
  }

  @Override
  public void stop() throws Exception {
    ServiceManager serviceManager = getApplicationManager().getServiceManager(DataCleansingService.NAME);
    serviceManager.stop();
    serviceManager.waitForStatus(false);
  }

  @Override
  public DataCleansingTestState getInitialState() {
    return new DataCleansingTestState(0, 0, 0, 0, 0);
  }

  @Override
  public void awaitOperations(DataCleansingTestState state) throws Exception {
    getApplicationManager().getMapReduceManager(DATACLEANSING_MAPREDUCE_NAME).waitForFinish(5, TimeUnit.MINUTES);
  }

  @Override
  public void verifyRuns(DataCleansingTestState state) throws Exception {
    LOG.info("verifying runs for data cleaning");
    // For now, check total number of clean records and invalid records
    Assert.assertEquals(state.getEndInvalidRecordPid(), getTotalRecords(true) + getTotalRecords(false));

    // verify segregated records
    Assert.assertTrue(verifyRecordsWithExplore(state));
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().toEntityId().app(DATACLEANSING_NAME));
  }

  @Override
  public DataCleansingTestState runOperations(DataCleansingTestState state) throws Exception {
    ApplicationManager applicationManager = getApplicationManager();
    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME);
    URL serviceURL = serviceManager.getServiceURL();

    LOG.info("Writing {} events in one batch", CLEAN_RECORDS_PER_BATCH + INVALID_RECORDS_PER_BATCH);
    // write a set of records to one partition and run the DataCleansingMapReduce job on that one partition
    createPartition(serviceURL, state);
    long now = System.currentTimeMillis();
    ImmutableMap<String, String> args = ImmutableMap.of(OUTPUT_PARTITION_KEY, Long.toString(now),
                                                        SCHEMA_KEY, SCHEMAJSON);
    applicationManager.getMapReduceManager(DATACLEANSING_MAPREDUCE_NAME).start(args);

    return new DataCleansingTestState(now, state.getEndInvalidRecordPid() + 1,
                                      state.getEndInvalidRecordPid() + CLEAN_RECORDS_PER_BATCH,
                                      state.getEndInvalidRecordPid() + CLEAN_RECORDS_PER_BATCH + 1,
                                      state.getEndInvalidRecordPid() + CLEAN_RECORDS_PER_BATCH +
                                      INVALID_RECORDS_PER_BATCH);
  }

  private void createPartition(URL serviceUrl, DataCleansingTestState state)
    throws IOException, UnauthenticatedException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    List<String> records = new ArrayList<>();
    generateRecords(records, state.getEndInvalidRecordPid() + 1, false);
    generateRecords(records, state.getEndInvalidRecordPid() + CLEAN_RECORDS_PER_BATCH + 1, true);
    String body = Joiner.on("\n").join(records) + "\n";
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void generateRecords(List<String> records, long start, boolean invalid) {
    long numRecords = invalid ? INVALID_RECORDS_PER_BATCH : CLEAN_RECORDS_PER_BATCH;
    for (long i = start; i < (start + numRecords); i++) {
      records.add(getRecord(i, invalid));
    }
  }

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
    QueryClient queryClient = new QueryClient(getClientConfig());
    String cleanRecordsQuery = "SELECT * FROM dataset_" + CLEAN_RECORDS_DATASET + " where TIME = "
      + state.getTimestamp();
    String invalidRecordsQuery = "SELECT * FROM dataset_" + INVALID_RECORDS_DATASET + " where TIME = "
      + state.getTimestamp();

    // Reduce wait time by submitting both the queries
    ListenableFuture<ExploreExecutionResult> cleanRecordsExecute = queryClient.execute(getLongRunningNamespace(),
                                                                                       cleanRecordsQuery);
    ListenableFuture<ExploreExecutionResult> invalidRecordsExecute = queryClient.execute(getLongRunningNamespace(),
                                                                                         invalidRecordsQuery);
    ExploreExecutionResult cleanRecordsResult = cleanRecordsExecute.get();
    ExploreExecutionResult invalidRecordsResult = invalidRecordsExecute.get();

    return (verifyResults(cleanRecordsResult, state.getStartCleanRecordPid(), false) &&
      verifyResults(invalidRecordsResult, state.getStartInvalidRecordPid(), true));
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
