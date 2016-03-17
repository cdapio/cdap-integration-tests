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

package co.cask.cdap.apps;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.wordcount.RetrieveCounts;
import co.cask.cdap.examples.wordcount.WordCount;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {link Dataset} which tests all the rest endpoints of dataset
 */
//TODO: Add test for datasets used by an adapter
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
    Id.DatasetInstance testDatasetinstance = Id.DatasetInstance.from(TEST_NAMESPACE, "testDataset");
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
    datasetClient.waitForDeleted(testDatasetinstance, 10, TimeUnit.SECONDS);
    Assert.assertEquals(appDatasetsCount, datasetClient.list(TEST_NAMESPACE).size());

    FlowManager flowManager = applicationManager.getFlowManager("WordCounter").start();
    flowManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    ServiceManager wordCountService = applicationManager.getServiceManager(RetrieveCounts.SERVICE_NAME).start();
    wordCountService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    StreamManager wordStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "wordStream"));
    wordStream.send("hello world");

    RuntimeMetrics flowletMetrics = flowManager.getFlowletMetrics("unique");
    flowletMetrics.waitForProcessed(1, PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // verify through service that there are 2 words
    Map<String, Object> responseMap = getWordCountStats(wordCountService);
    Assert.assertEquals(2.0, ((double) responseMap.get("totalWords")), 0);
    // test truncating a dataset with existing dataset
    datasetClient.truncate(Id.DatasetInstance.from(TEST_NAMESPACE, "wordStats"));
    // after truncating there should be 0 word
    responseMap = getWordCountStats(wordCountService);
    Assert.assertEquals(0, ((double) responseMap.get("totalWords")), 0);

    // test the number of datasets used by an app with existing app
    Assert.assertEquals(appDatasetsCount, getDatasetInstances(String.format("apps/%s/datasets",
                                                                           WordCount.class.getSimpleName())).size());

    // test the number of datasets used by an app with non existing app
    Assert.assertEquals(0, getDatasetInstances(String.format("apps/%s/datasets", "nonExistingApp")).size());


    // test datasets used by a program with an existing program
    Assert.assertEquals(appDatasetsCount, getDatasetInstances(String.format("apps/%s/flows/%s/datasets",
                                                                            WordCount.class.getSimpleName(),
                                                                            "WordCounter")).size());

    // test datasets used by a program with a non existing program
    Assert.assertEquals(0, getDatasetInstances(String.format("apps/%s/flows/%s/datasets",
                                                             WordCount.class.getSimpleName(),
                                                             "nonExistingProgram")).size());

    // test programs using a dataset with existing dataset name
    Assert.assertEquals(2, getPrograms(String.format("data/datasets/%s/programs", "wordStats")).size());


    // test programs using a dataset with non existing dataset name
    Assert.assertEquals(0, getPrograms(String.format("data/datasets/%s/programs",
                                                     "nonExistingDataset")).size());
  }

  private Map<String, Object> getWordCountStats(ServiceManager wordCountService) throws Exception {
    URL url = new URL(wordCountService.getServiceURL(), "stats");
    // we have to make the first handler call after service starts with a retry
    HttpResponse response = retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(),
                         new TypeToken<Map<String, Object>>() { }.getType());
  }

  private Set<Id.Program> getPrograms(String endPoint) throws IOException, UnauthenticatedException {
    HttpResponse response = makeRequest(endPoint);
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Set<Id.Program>>() {
    }.getType());
  }

  private Set<Id.DatasetInstance> getDatasetInstances(String endPoint) throws IOException, UnauthenticatedException {
    HttpResponse response = makeRequest(endPoint);
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Set<Id.DatasetInstance>>() {
    }.getType());
  }

  private HttpResponse makeRequest(String endPoint) throws IOException, UnauthenticatedException {
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveNamespacedURLV3(TEST_NAMESPACE, endPoint);
    HttpResponse response = getRestClient().execute(HttpMethod.GET, url, clientConfig.getAccessToken());
    Assert.assertEquals(response.getResponseCode(), HttpURLConnection.HTTP_OK);
    return response;
  }
}
