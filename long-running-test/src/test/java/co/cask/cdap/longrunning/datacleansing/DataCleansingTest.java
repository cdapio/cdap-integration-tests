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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.examples.datacleansing.DataCleansing;
import co.cask.cdap.examples.datacleansing.DataCleansingMapReduce;
import co.cask.cdap.examples.datacleansing.DataCleansingService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Data Cleansing long running test
 */
public class DataCleansingTest extends LongRunningTestBase<DataCleansingTestState> {
  private static final int BATCH_SIZE = 100;
  private static final String DATACLEANSING_MAPREDUCE_NAME = "DataCleansingMapReduce";
  private static final String DATACLEANSING_NAME = "DataCleansing";
  private static final String OUTPUT_PARTITION_KEY = "output.partition.key";
  private static final String SCHEMA_KEY = "schema.key";
  private static final String SCHEMAJSON = DataCleansingMapReduce.SchemaMatchingFilter.DEFAULT_SCHEMA.toString();

  private static final Set<String> RECORDS1 =
    ImmutableSet.of("{\"pid\":223986723,\"name\":\"bob\",\"dob\":\"02-12-1983\",\"zip\":\"84125\"}",
                    "{\"pid\":198637201,\"name\":\"timothy\",\"dob\":\"06-21-1995\",\"zip\":\"84125q\"}");


  @Override
  public void setup() throws Exception {
    LOG.info("setting up data cleaning test...");
    ApplicationManager applicationManager = deployApplication(DataCleansing.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME).start();
    serviceManager.waitForStatus(true);
    URL serviceURL = serviceManager.getServiceURL();
    TimeUnit.SECONDS.sleep(30);
  }

  @Override
  public void cleanup() throws Exception {
    // Nothing to do for now
    LOG.info("cleaning up data cleansing test....");
  }

  @Override
  public DataCleansingTestState getInitialState() {
    return new DataCleansingTestState(0);
  }

  @Override
  public void verifyRuns(DataCleansingTestState state) throws Exception {
    // for now, check if sum of invalid and valid records is equal to preserved state of test
    LOG.info("verifying runs for data cleaning");
    long validRecords = getValidityMetrics(false);
    long invalidRecords = getValidityMetrics(true);
    Assert.assertEquals(state.getTotalRecords(), validRecords + invalidRecords);
  }

  @Override
  public DataCleansingTestState runOperations(DataCleansingTestState state) throws Exception {
    // TODO: Deploy app only once in setup()
    ApplicationManager applicationManager = deployApplication(DataCleansing.class);
    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME);
    URL serviceURL = serviceManager.getServiceURL();

    LOG.info("Writing {} events in one batch", (2 * BATCH_SIZE));

    // write a set of records to one partition and run the DataCleansingMapReduce job on that one partition
    createPartition(serviceURL);
    Long now = System.currentTimeMillis();
    ImmutableMap<String, String> args = ImmutableMap.of(OUTPUT_PARTITION_KEY, now.toString(),
                                                        SCHEMA_KEY, SCHEMAJSON);
    MapReduceManager mapReduceManager = applicationManager.
      getMapReduceManager(DATACLEANSING_MAPREDUCE_NAME).start(args);
    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);

    long newTotalRecords = state.getTotalRecords() + (2 * BATCH_SIZE);
    return new DataCleansingTestState(newTotalRecords);
  }

  private void createPartition(URL serviceUrl) throws IOException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BATCH_SIZE; i++) {
      sb.append(Joiner.on("\n").join(RECORDS1));
      sb.append("\n");
    }
    HttpRequest request = HttpRequest.post(url).withBody(sb.toString()).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  // pass true to get the number of invalid records; pass false to get the number of valid records processed.
  private long getValidityMetrics(boolean invalid) throws Exception {
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
}
