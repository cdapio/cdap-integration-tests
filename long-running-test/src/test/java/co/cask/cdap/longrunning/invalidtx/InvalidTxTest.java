/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.longrunning.invalidtx;

import co.cask.cdap.longrunning.invalidtx.app.PartitionCreateWorker;
import co.cask.cdap.longrunning.invalidtx.app.PartitionScannerHandler;
import co.cask.cdap.longrunning.invalidtx.app.PartitionTableDebugApp;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.Assert;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InvalidTxTest extends LongRunningTestBase<InvalidTxState> {
  private static final Gson GSON = new Gson();
  private static final String concurrency = "20";
  private static final int numPartitions = 10;
  private static final String numPartitionsString = Integer.toString(numPartitions);

  @Override
  public void deploy() throws Exception {
    deployApplication(PartitionTableDebugApp.class);
  }

  @Override
  public void start() throws Exception {
    getApplicationManager().getServiceManager(PartitionTableDebugApp.SERVICE_NAME).start();
  }

  @Override
  public void stop() throws Exception {
    ServiceManager serviceManager = getApplicationManager().getServiceManager(PartitionTableDebugApp.SERVICE_NAME);
    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, 5, TimeUnit.SECONDS);
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().app(PartitionTableDebugApp.APP_NAME));
  }

  @Override
  public InvalidTxState getInitialState() {
    return new InvalidTxState(null, 0);
  }

  @Override
  public void awaitOperations(InvalidTxState state) throws Exception {
    WorkerManager workerManager =
      getApplicationManager().getWorkerManager(PartitionCreateWorker.class.getSimpleName());
    workerManager.waitForRuns(ProgramRunStatus.COMPLETED, state.getWorkerRuns(), 2, TimeUnit.MINUTES);
  }

  @Override
  public void verifyRuns(InvalidTxState state) throws Exception {
    ServiceManager serviceManager = getApplicationManager().getServiceManager(PartitionTableDebugApp.SERVICE_NAME);
    URL baseURL = serviceManager.getServiceURL();
    URL url = new URL(baseURL, "pfs/" + PartitionTableDebugApp.RAW_RECORDS);
    HttpResponse httpResponse = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    PartitionScannerHandler.Response response =
      GSON.fromJson(httpResponse.getResponseBodyAsString(), PartitionScannerHandler.Response.class);
    Assert.assertTrue(response.isReachedEndOfScan());
    Assert.assertEquals("Found entries in index, for which data was not present: " + response.getEmptyDataRows(),
                        0, response.getEmptyDataRowCount());
    Assert.assertEquals(state.getWorkerRuns() * numPartitions, response.getRowsProcessed());
  }

  @Override
  public InvalidTxState runOperations(InvalidTxState state) throws Exception {
    long timeToAdd = state.getLastTimeAdded() == null ? 0 : state.getLastTimeAdded() + 1;
    WorkerManager workerManager =
      getApplicationManager().getWorkerManager(PartitionCreateWorker.class.getSimpleName());
    workerManager.start(ImmutableMap.of("time", Long.toString(timeToAdd),
                                        "concurrency", concurrency, "numPartitions", numPartitionsString));
    return new InvalidTxState(timeToAdd + numPartitions - 1, state.getWorkerRuns() + 1);
  }
}
