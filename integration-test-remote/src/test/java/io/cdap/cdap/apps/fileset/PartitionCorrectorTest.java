/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.apps.fileset;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests that the partition corrector worker can recreate missing partitions, with or without dropping the hive table.
 */
public class PartitionCorrectorTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(PFSApp.class);

    ServiceManager pfsService = applicationManager.getServiceManager("PFSService");
    startAndWaitForRun(pfsService, ProgramRunStatus.RUNNING);
    URL serviceURL = pfsService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    for (int i = 0; i < 100; i++) {
      HttpResponse response = getRestClient()
        .execute(HttpRequest.put(new URL(serviceURL, String.valueOf(i))).withBody("").build(),
                 getClientConfig().getAccessToken());
      Assert.assertEquals(200, response.getResponseCode());
    }

    QueryClient queryClient = new QueryClient(getClientConfig());
    validate(queryClient, 100);

    dropPartitionsAndCorrect(applicationManager, ImmutableMap.of("batch.size", "20"));
    dropPartitionsAndCorrect(applicationManager, ImmutableMap.of("disable.explore", "false", "batch.size", "20"));
  }

  private void dropPartitionsAndCorrect(ApplicationManager applicationManager, Map<String, String> args)
    throws TimeoutException, InterruptedException, ExecutionException {

    // delete about half of the partitions directly in hive
    QueryClient queryClient = new QueryClient(getClientConfig());
    ExploreExecutionResult results = queryClient
      .execute(TEST_NAMESPACE, "alter table dataset_pfs drop partition (key>'49')").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    validate(queryClient, 45); // (5-9, 50-99 were dropped)

    // run the partition corrector. This should bring all partitions back
    WorkerManager pfsWorker = applicationManager.getWorkerManager("PartitionWorker");
    startAndWaitForRun(pfsWorker, ProgramRunStatus.COMPLETED,
                       ImmutableMap.<String, String>builder().put("dataset.name", "pfs").putAll(args).build(),
                       PROGRAM_START_STOP_TIMEOUT_SECONDS + 100, TimeUnit.SECONDS);
    validate(queryClient, 100);
  }

  private void validate(QueryClient client, int expected) throws ExecutionException, InterruptedException {

    ExploreExecutionResult results = client.execute(TEST_NAMESPACE, "show partitions dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertEquals(expected, Iterators.size(results));

    results = client.execute(TEST_NAMESPACE, "select * from dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertEquals(expected, Iterators.size(results));
  }
}
