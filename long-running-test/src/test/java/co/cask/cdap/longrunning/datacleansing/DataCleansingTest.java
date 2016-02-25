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

import co.cask.cdap.client.QueryClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.examples.datacleansing.DataCleansing;
import co.cask.cdap.examples.datacleansing.DataCleansingMapReduce;
import co.cask.cdap.examples.datacleansing.DataCleansingService;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Data Cleansing long running test
 */
public class DataCleansingTest extends LongRunningTestBase<DataCleansingTestState> {
  private static final int NO_OF_CLEAN_RECORDS = 70;
  private static final int NO_OF_INVALID_RECORDS = 30;

  private static final String DATACLEANSING_MAPREDUCE_NAME = "DataCleansingMapReduce";
  private static final String DATACLEANSING_NAME = "DataCleansing";
  private static final String OUTPUT_PARTITION_KEY = "output.partition.key";
  private static final String SCHEMA_KEY = "schema.key";
  private static final String SCHEMAJSON = DataCleansingMapReduce.SchemaMatchingFilter.DEFAULT_SCHEMA.toString();

  private static final String CLEAN_RECORDS_DATASET = "cleanRecords";
  private static final String INVALID_RECORDS_DATASET = "invalidRecords";

  private static final String CLEAN_RECORD = ",\"name\":\"bob\",\"dob\":\"02-12-1983\",\"zip\":\"84125\"}";
  private static final String INVALID_RECORD = ",\"name\":\"timothy\",\"dob\":\"06-21-1995\",\"zip\":\"84125q\"}";

  @Override
  public void setup() throws Exception {
    LOG.info("setting up data cleaning test");
    ApplicationManager applicationManager = deployApplication(DataCleansing.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME).start();
    serviceManager.waitForStatus(true);
    TimeUnit.SECONDS.sleep(30);
  }

  // TODO: Clean partitions and keyvaluetable
  @Override
  public void cleanup() throws Exception {
    LOG.info("cleaning up data cleansing test");
  }

  @Override
  public DataCleansingTestState getInitialState() {
    return new DataCleansingTestState(0, 0, 0, 0, 0);
  }

  @Override
  public void verifyRuns(DataCleansingTestState state) throws Exception {
    LOG.info("verifying runs for data cleaning");
    // For now, check total number of clean records and invalid records
    Assert.assertEquals(state.getEndInvalidRecordPid(), verifyMetrics(true) + verifyMetrics(false));

    // verify segregated records
    Assert.assertTrue(verifyRecordsWithExplore(state));
  }

  @Override
  public DataCleansingTestState runOperations(DataCleansingTestState state) throws Exception {
    ApplicationManager applicationManager = getApplicationManager(Id.Application.from(Id.Namespace.DEFAULT.getId(),
                                                                                      DATACLEANSING_NAME));
    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME);
    URL serviceURL = serviceManager.getServiceURL();

    LOG.info("Writing {} events in one batch", NO_OF_CLEAN_RECORDS + NO_OF_INVALID_RECORDS);
    // write a set of records to one partition and run the DataCleansingMapReduce job on that one partition
    createPartition(serviceURL, state);
    Long now = System.currentTimeMillis();
    ImmutableMap<String, String> args = ImmutableMap.of(OUTPUT_PARTITION_KEY, now.toString(),
                                                        SCHEMA_KEY, SCHEMAJSON);
    applicationManager.getMapReduceManager(DATACLEANSING_MAPREDUCE_NAME).start(args);

    return new DataCleansingTestState(now, state.getEndInvalidRecordPid() + 1,
                                      state.getEndInvalidRecordPid() + NO_OF_CLEAN_RECORDS,
                                      state.getEndInvalidRecordPid() + NO_OF_CLEAN_RECORDS + 1,
                                      state.getEndInvalidRecordPid() + NO_OF_CLEAN_RECORDS + NO_OF_INVALID_RECORDS);
  }

  private void createPartition(URL serviceUrl, DataCleansingTestState state) throws IOException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    StringBuilder sb = new StringBuilder();
    generateRecords(sb, state.getEndInvalidRecordPid() + 1, false);
    generateRecords(sb, state.getEndInvalidRecordPid() + NO_OF_CLEAN_RECORDS + 1, true);
    HttpRequest request = HttpRequest.post(url).withBody(sb.toString()).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void generateRecords(StringBuilder sb, long start, boolean invalid) {
    long noOfRecords = invalid ? NO_OF_INVALID_RECORDS : NO_OF_CLEAN_RECORDS;
    String record = invalid ? INVALID_RECORD : CLEAN_RECORD;
    for (long i = start; i < (start + noOfRecords); i++) {
      createRecord(sb, record, i);
      sb.append("\n");
    }
  }

  private void createRecord(StringBuilder sb, String record, long i) {
    sb.append("{\"pid\":");
    sb.append(i);
    sb.append(record);
  }

  // pass true to get the number of invalid records; pass false to get the number of valid records processed.
  private long verifyMetrics(boolean invalid) throws Exception {
    String metric = "user.records." + (invalid ? "invalid" : "valid");
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Id.Namespace.DEFAULT.getId(),
                                               Constants.Metrics.Tag.APP, DATACLEANSING_NAME,
                                               Constants.Metrics.Tag.MAPREDUCE, DATACLEANSING_MAPREDUCE_NAME);
    MetricQueryResult result = getMetricsClient().query(tags, metric);
    // copied from MetricsClient#getTotalCounter
    if (result.getSeries().length == 0) {
      return 0L;
    } else {
      MetricQueryResult.TimeValue[] timeValues = result.getSeries()[0].getData();
      return timeValues.length == 0 ? 0L : timeValues[0].getValue();
    }
  }

  // TODO: Use serivce instead of explore
  private boolean verifyRecordsWithExplore(DataCleansingTestState state) throws Exception {
    QueryClient queryClient = new QueryClient(getClientConfig());
    String cleanRecordsQuery = "SELECT * FROM dataset_" + CLEAN_RECORDS_DATASET + " where TIME = "
                                + state.getTimestamp();
    String invalidRecordsQuery = "SELECT * FROM dataset_" + INVALID_RECORDS_DATASET + " where TIME = "
                                + state.getTimestamp();

    // Reduce wait time by submitting both the queries
    ListenableFuture<ExploreExecutionResult> cleanRecordsExecute = queryClient.execute(TEST_NAMESPACE,
                                                                                       cleanRecordsQuery);
    ListenableFuture<ExploreExecutionResult> invalidRecordsExecute = queryClient.execute(TEST_NAMESPACE,
                                                                                       invalidRecordsQuery);
    ExploreExecutionResult cleanRecordsResult = cleanRecordsExecute.get();
    ExploreExecutionResult invalidRecordsResult = invalidRecordsExecute.get();

    state.getStartInvalidRecordPid();
    return (verifyResults(cleanRecordsResult, CLEAN_RECORD, state.getStartCleanRecordPid()) &&
            verifyResults(invalidRecordsResult, INVALID_RECORD, state.getStartInvalidRecordPid()));
  }

  private boolean verifyResults(ExploreExecutionResult result, String record, long i) {
    while (result.hasNext()) {
      QueryResult next = result.next();
      List<Object> columns = next.getColumns();
      StringBuilder sb = new StringBuilder();
      createRecord(sb, record, i);
      String expectedRecord = sb.toString();
      if (!expectedRecord.equalsIgnoreCase((String) columns.get(0))) {
        return false;
      }
      i++;
    }
    return true;
  }
}
