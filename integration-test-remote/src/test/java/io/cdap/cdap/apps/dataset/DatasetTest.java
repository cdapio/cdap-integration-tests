/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.apps.dataset;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for datasets which tests all the rest endpoints of datasets.
 */
public class DatasetTest extends AudiTestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {

    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());

    // there should be no datasets in the test namespace
    Assert.assertEquals(0, datasetClient.list(TEST_NAMESPACE).size());

    ApplicationManager applicationManager = deployApplication(WordCount.class);

    // number of datasets which were created by the wordcount app
    int appDatasetsCount = datasetClient.list(TEST_NAMESPACE).size();

    // test creating dataset
    DatasetId testDatasetinstance = TEST_NAMESPACE.dataset("testDataset");
    datasetClient.create(testDatasetinstance, "table");

    // one more dataset should have been added
    Assert.assertEquals(appDatasetsCount + 1, datasetClient.list(TEST_NAMESPACE).size());

    // test that properties there is nothing in properties in the new dataset created above
    DatasetMeta oldMeta = datasetClient.get(testDatasetinstance);
    Assert.assertEquals(0, oldMeta.getSpec().getProperties().size());

    // update the dataset properties
    datasetClient.update(testDatasetinstance, ImmutableMap.of("fruit", "mango", "one", "1"));

    // test if properties in meta got updated
    DatasetMeta newMeta = datasetClient.get(testDatasetinstance);
    Assert.assertEquals(2, newMeta.getSpec().getProperties().size());

    // test if properties in meta is correct
    Assert.assertTrue(newMeta.getSpec().getProperties().containsKey("fruit"));
    Assert.assertTrue(newMeta.getSpec().getProperties().containsKey("one"));
    Assert.assertEquals("mango", newMeta.getSpec().getProperties().get("fruit"));
    Assert.assertEquals("1", newMeta.getSpec().getProperties().get("one"));

    // test deleting a datatset
    datasetClient.delete(testDatasetinstance);
    Assert.assertEquals(appDatasetsCount, datasetClient.list(TEST_NAMESPACE).size());

    ServiceManager wordCountService = applicationManager.getServiceManager(RetrieveCounts.SERVICE_NAME).start();
    wordCountService.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    ingestData();

    Map<String, Object> responseMap = getWordCountStats(wordCountService);
    Assert.assertEquals(2.0, ((double) responseMap.get("totalWords")), 0);
    // test truncating a dataset with existing dataset
    datasetClient.truncate(TEST_NAMESPACE.dataset("wordStats"));
    // after truncating there should be 0 word
    responseMap = getWordCountStats(wordCountService);
    Assert.assertEquals(0, ((double) responseMap.get("totalWords")), 0);

    // test the number of datasets used by an app with existing app
    Assert.assertEquals(appDatasetsCount, getDatasetInstances(String.format("apps/%s/datasets",
                                                                            WordCount.class.getSimpleName())).size());

    // test the number of datasets used by an app with non existing app
    Assert.assertEquals(0, getDatasetInstances(String.format("apps/%s/datasets", "nonExistingApp")).size());


    // test datasets used by a program with an existing program
    Assert.assertEquals(appDatasetsCount, getDatasetInstances(String.format("apps/%s/datasets",
                                                                            WordCount.class.getSimpleName(),
                                                                            "WordCounter")).size());

    // test datasets used by a program with a non existing program (one since we write to the dataset with a non
    // existing program).
    Assert.assertEquals(1, getDatasetInstances(String.format("apps/%s/datasets",
                                                             WordCount.class.getSimpleName(),
                                                             "nonExistingProgram")).size());

    // test programs using a dataset with existing dataset name
    Assert.assertEquals(2, getPrograms(String.format("data/datasets/%s/programs", "wordStats")).size());


    // test programs using a dataset with non existing dataset name
    Assert.assertEquals(0, getPrograms(String.format("data/datasets/%s/programs",
                                                     "nonExistingDataset")).size());
  }

  private Map<String, Object> getWordCountStats(ServiceManager wordCountService) throws Exception {
    URL url = new URL(wordCountService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS), "stats");
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(),
                         new TypeToken<Map<String, Object>>() { }.getType());
  }

  private Set<ProgramId> getPrograms(String endPoint)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = makeRequest(endPoint);
    return GSON.fromJson(response.getResponseBodyAsString(),
                         new TypeToken<Set<ProgramId>>() { }.getType());
  }

  private Set<DatasetId> getDatasetInstances(String endPoint)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    HttpResponse response = makeRequest(endPoint);
    return GSON.fromJson(response.getResponseBodyAsString(),
                         new TypeToken<Set<DatasetId>>() { }.getType());
  }

  private HttpResponse makeRequest(String endPoint)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveNamespacedURLV3(TEST_NAMESPACE, endPoint);
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, clientConfig.getAccessToken());
    Assert.assertEquals(response.getResponseCode(), HttpURLConnection.HTTP_OK);
    return response;
  }

  protected void ingestData() throws Exception {
    // write input data
    DataSetManager<Table> datasetManager = getTableDataset("wordStats");
    Table table = datasetManager.get();
    Put put = new Put("totals");
    put.add("total_length", 2L);
    put.add("total_words", 2L);
    table.put(put);
    datasetManager.flush();
  }
}
