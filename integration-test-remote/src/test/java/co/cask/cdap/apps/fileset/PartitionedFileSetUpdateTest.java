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

package co.cask.cdap.apps.fileset;

import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tests that updating the properties of a PartitionedFileSet does not remove its partitions.
 */
@Category({
  // Do not run the tests on the distros which has older version of hive.
  HDP20Incompatible.class,
  CDH51Incompatible.class
})
public class PartitionedFileSetUpdateTest extends AudiTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetUpdateTest.class);

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(PFSApp.class);

    ServiceManager pfsService = applicationManager.getServiceManager("PFSService").start();
    pfsService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceURL = pfsService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    HttpResponse response = getRestClient().execute(HttpRequest.put(new URL(serviceURL, "1")).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    response = getRestClient().execute(HttpMethod.PUT, new URL(serviceURL, "2"), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    response = getRestClient().execute(HttpMethod.PUT, new URL(serviceURL, "3"), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());
    QueryClient client = new QueryClient(getClientConfig());
    validate(client, "a", "b", 3, 3);

    // update description and validate everything is still there
    datasetClient.updateExisting(TEST_NAMESPACE.dataset("pfs"),
                                 PartitionedFileSetProperties.builder()
                                   .setDescription("updated description").build().getProperties());
    validate(client, "a", "b", 3, 3);

    // update input format and validate everything is still there (input format has not influence on explore)
    datasetClient.updateExisting(TEST_NAMESPACE.dataset("pfs"),
                                 PartitionedFileSetProperties.builder()
                                   .setInputFormat(FileInputFormat.class).build().getProperties());
    validate(client, "a", "b", 3, 3);

    // update the schema and validate the new explore schema, and everything is still there
    datasetClient.updateExisting(TEST_NAMESPACE.dataset("pfs"),
                                 PartitionedFileSetProperties.builder()
                                   .setExploreSchema("i string, j string").build().getProperties());
    validate(client, "i", "j", 3, 3);

    // update delimiter and validate everything is still there. Note: the change does not affect existing partitions
    datasetClient.updateExisting(TEST_NAMESPACE.dataset("pfs"),
                                 PartitionedFileSetProperties.builder().setExploreFormat("text")
                                   .setExploreFormatProperty("delimiter", ":").build().getProperties());

    validate(client, "i", "j", 3, 3);

    // add a new partition and validate that the delimiter change applied to it
    response = getRestClient().execute(HttpMethod.PUT, new URL(serviceURL, "4"), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    validate(client, "i", "j", 3, 4);

    // run the partition corrector. This should bring all partitions to use delimiter :
    WorkerManager pfsWorker = applicationManager
      .getWorkerManager("PartitionWorker").start(ImmutableMap.of("dataset.name", "pfs"));
    pfsWorker.waitForRun(ProgramRunStatus.COMPLETED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    validate(client, "i", "j", 0, 4);
  }

  /**
   * This validates that the pfs has all data in the correct format. Assumes that the pfs has a hybrid set
   * of partitions, up to a certain partition key (commaLimit) delimited by comma, thereafter delimited by :
   *
   * @param client a query client
   * @param fieldA the name of the first field in the explore table schema
   * @param fieldB  the name of the second field in the explore table schema
   * @param commaLimit partitions from key=1 up to key=commaLimit (inclusive) have ',' as the delimiter
   * @param colonLimit partitions with key>commaLimit up to key=colonLimit have ':' as the delimiter
   */
  private void validate(QueryClient client, String fieldA, String fieldB, int commaLimit, int colonLimit)
    throws ExecutionException, InterruptedException, ExploreException {

    List<List<String>> expectedResults = new ArrayList<>();
    for (int i = 1; i <= colonLimit; i++) {
      expectedResults.add(ImmutableList.of(
        i <= commaLimit ? String.valueOf(i) : String.format("%d,%d", i, i),
        i <= commaLimit ? String.format("%d:%d", i, i) : String.valueOf(i),
        String.valueOf(i)
      ));
    }

    ExploreExecutionResult results = client.execute(TEST_NAMESPACE, "show partitions dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    List<List<Object>> rows = executionResult2Rows(results);
    LOG.info("rows: {}", new Gson().toJson(rows));
    Assert.assertEquals(expectedResults.size(), rows.size());

    results = client.execute(TEST_NAMESPACE, "select * from dataset_pfs").get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    rows = executionResult2Rows(results);
    LOG.info("rows: {}", new Gson().toJson(rows));
    Assert.assertEquals(expectedResults.size(), rows.size());
    Iterator<List<Object>> iter = rows.iterator();
    for (List<String> row : expectedResults) {
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(row, iter.next());
    }
    Assert.assertFalse(iter.hasNext());

    LOG.info("rows: {}", new Gson().toJson(results.getResultSchema()));
    Iterator<ColumnDesc> columns = results.getResultSchema().iterator();
    Assert.assertTrue(columns.hasNext());
    Assert.assertEquals("dataset_pfs." + fieldA, columns.next().getName());
    Assert.assertTrue(columns.hasNext());
    Assert.assertEquals("dataset_pfs." + fieldB, columns.next().getName());
    Assert.assertTrue(columns.hasNext());
    Assert.assertEquals("dataset_pfs.key", columns.next().getName());
    Assert.assertFalse(columns.hasNext());
  }

  private List<List<Object>> executionResult2Rows(ExploreExecutionResult executionResult) {
    List<List<Object>> rows = Lists.newArrayList();
    while (executionResult.hasNext()) {
      rows.add(executionResult.next().getColumns());
    }
    return rows;
  }

}
