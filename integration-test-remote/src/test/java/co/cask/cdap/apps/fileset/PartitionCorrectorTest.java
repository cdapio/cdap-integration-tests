/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.apps.fileset;

import co.cask.cdap.client.QueryClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests that the partition corrector worker can recreate missing partitions, with or without dropping the hive table.
 */
@Category({
  // Do not run the tests on the distros which has older version of hive.
  HDP20Incompatible.class,
  CDH51Incompatible.class
})
public class PartitionCorrectorTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(PFSApp.class);

    ServiceManager pfsService = applicationManager.getServiceManager("PFSService").start();
    pfsService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceURL = pfsService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    for (int i = 0; i < 50; i++) {
      HttpResponse response = getRestClient().execute(HttpRequest.put(new URL(serviceURL, String.valueOf(i))).build(),
                                                      getClientConfig().getAccessToken());
      Assert.assertEquals(200, response.getResponseCode());
    }

    QueryClient queryClient = new QueryClient(getClientConfig());
    validate(queryClient, 50);

    dropPartitionsAndCorrect(applicationManager, ImmutableMap.of("batch.size", "20"));
    dropPartitionsAndCorrect(applicationManager, ImmutableMap.of("disable.explore", "false", "batch.size", "20"));
  }

  private void dropPartitionsAndCorrect(ApplicationManager applicationManager, Map<String, String> args)
    throws TimeoutException, InterruptedException, ExploreException, ExecutionException {

    // delete about half of the partitions directly in hive
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult results = queryClient
      .execute(TEST_NAMESPACE, "alter table dataset_pfs drop partition (key>'29')").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    validate(queryClient, 23); // (2-9, 30-49 were dropped)

    // run the partition corrector. This should bring all partitions back
    WorkerManager pfsWorker = applicationManager.getWorkerManager("PartitionWorker");
    List<RunRecord> history = pfsWorker.getHistory();
    pfsWorker.start(ImmutableMap.<String, String>builder().put("dataset.name", "pfs").putAll(args).build());
    pfsWorker.waitForRuns(ProgramRunStatus.COMPLETED, history.size() + 1,
                          PROGRAM_START_STOP_TIMEOUT_SECONDS + 100, TimeUnit.SECONDS);
    validate(queryClient, 50);
  }

  private void validate(QueryClient client, int expected)
    throws ExecutionException, InterruptedException, ExploreException {

    ExploreExecutionResult results = client.execute(TEST_NAMESPACE, "show partitions dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertEquals(expected, Iterators.size(results));

    results = client.execute(TEST_NAMESPACE, "select * from dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertEquals(expected, Iterators.size(results));
  }
}
