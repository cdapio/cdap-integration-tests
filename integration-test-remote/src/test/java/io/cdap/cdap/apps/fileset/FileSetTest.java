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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality of {@link io.cdap.cdap.data2.dataset2.lib.file.FileSetDataset}
 */
public class FileSetTest extends AudiTestBase {
  private static final List<String> DATA_LIST =
    Lists.newArrayList("Hello World", "My Hello Hello World", "World Hello");
  private static final String DATA_UPLOAD = Joiner.on("\n").join(DATA_LIST);

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(FileSetExample.class);
    DatasetClient datasetClient = getDatasetClient();
    List<DatasetSpecificationSummary> datasetSpecificationsList = datasetClient.list(TEST_NAMESPACE);
    Assert.assertEquals(2, datasetSpecificationsList.size());
    for (DatasetSpecificationSummary datasetSpecificationSummary : datasetSpecificationsList) {
      Assert.assertEquals(FileSet.class.getName(), datasetSpecificationSummary.getType());
      String datasetName = datasetSpecificationSummary.getName();
      Assert.assertTrue("Dataset list consists of dataset other than 'lines' and 'count': "
                          + new Gson().toJson(datasetSpecificationsList),
                        "lines".equals(datasetName) || "counts".equals(datasetName));
    }
    ServiceManager fileSetService = applicationManager.getServiceManager("FileSetService");
    startAndWaitForRun(fileSetService, ProgramRunStatus.RUNNING);

    URL serviceURL = fileSetService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL, "lines?path=myFile.txt");

    HttpResponse response = getRestClient().execute(HttpRequest.put(url).withBody(DATA_UPLOAD).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(DATA_UPLOAD, response.getResponseBodyAsString());


    // ensure that 400 is returned for a GET of a nonexistent file
    url = new URL(serviceURL, "lines?path=NoSuchFile.txt");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken(), 400);
    Assert.assertEquals(400, response.getResponseCode());

    // ensure that 400 is returned for a GET of a nonexistent dataset
    url = new URL(serviceURL, "NoSuchDataset?path=myFile.txt");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken(), 400);
    Assert.assertEquals(400, response.getResponseCode());


    MapReduceManager wordCountManager = applicationManager.getMapReduceManager("WordCount");

    // wait 5 minutes for mapreduce to complete
    wordCountManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    startAndWaitForRun(wordCountManager, ProgramRunStatus.COMPLETED,
                       ImmutableMap.of("dataset.lines.input.paths", "myFile.txt",
                                       "dataset.counts.output.path", "out.txt"),
                       5, TimeUnit.MINUTES);

    url = new URL(serviceURL, "counts?path=out.txt/part-r-00000");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    checkOutput(response.getResponseBodyAsString(), computeResult(DATA_LIST));
  }

  private Map<String, Integer> computeResult(List<String> data) {
    Map<String, Integer> resultMap = Maps.newHashMap();
    for (String words : data) {
      for (String word : words.split("\\s+")) {
        if (!resultMap.containsKey(word)) {
          resultMap.put(word, 1);
        } else {
          resultMap.put(word, resultMap.get(word) + 1);
        }
      }
    }
    return resultMap;
  }

  private void checkOutput(String output, Map<String, Integer> expectedMap) {
    String[] parts = output.trim().split("\\n");
    Assert.assertEquals(expectedMap.keySet().size(), parts.length);
    for (String part : parts) {
      String[] keyVal = part.split(":");
      Assert.assertEquals(2, keyVal.length);
      Assert.assertEquals(expectedMap.get(keyVal[0]), Integer.valueOf(keyVal[1]));
    }
  }
}
