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

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.examples.fileset.FileSetExample;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FileSetTest extends AudiTestBase {
  private static final List<String> DATA_LIST =
    Lists.newArrayList("Hello World", "My Hello Hello World", "World Hello");
  private static final String DATA_UPLOAD = Joiner.on("\n").join(DATA_LIST);

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(FileSetExample.class);
    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());
    List<DatasetSpecificationSummary> list = datasetClient.list();
    Assert.assertEquals(2, list.size());
    for (DatasetSpecificationSummary datasetSpecificationSummary : list) {
      Assert.assertEquals(FileSet.class.getName(), datasetSpecificationSummary.getType());
      String datasetName = datasetSpecificationSummary.getName();
      Assert.assertTrue("Dataset list consists of dataset other than 'lines' and 'count': " + new Gson().toJson(list),
                        "lines".equals(datasetName) || "counts".equals(datasetName));
    }
    ServiceManager fileSetService = applicationManager.startService("FileSetService");

    fileSetService.waitForStatus(true, 60, 1);

    // TODO: better way to wait for service to be up.
    TimeUnit.SECONDS.sleep(20);

    URL serviceURL = fileSetService.getServiceURL();
    URL url = new URL(serviceURL, "lines?path=myFile.txt");
    HttpResponse response =
      getRestClient().execute(HttpMethod.PUT, url, DATA_UPLOAD, null, getClientConfig().getAccessToken());
    // check for 200 shouldn't be necessary? (restClient already checks 200<=x<300
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


    MapReduceManager wordCountManager =
      applicationManager.startMapReduce("WordCount", ImmutableMap.of("dataset.lines.input.paths", "myFile.txt",
                                                                     "dataset.counts.output.path", "out.txt"));

    // mapreduce should start and then complete
    wordCountManager.waitForStatus(true, 60, 1);
    wordCountManager.waitForStatus(false, 5 * 60, 1);

    url = new URL(serviceURL, "counts?path=out.txt/part-r-00000");
    response = getRestClient().execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    checkOutput(response.getResponseBodyAsString(), computeResult(DATA_LIST));
  }

  private Map<String, Integer> computeResult(List<String> data) {
    Map<String, Integer> resultMap = Maps.newHashMap();
    for (String words : data) {
      for (String word : words.split("\\ ")) {
        Integer value = resultMap.get(word);
        if (value == null) {
          resultMap.put(word, 1);
        } else {
          resultMap.put(word, value + 1);
        }
      }
    }
    return resultMap;
  }

  private void checkOutput(String s, Map<String, Integer> expectedMap) {
    String[] parts = s.trim().split("\\n");
    Assert.assertEquals(expectedMap.keySet().size(), parts.length);
    for (String part : parts) {
      String[] keyVal = part.split(":");
      Assert.assertEquals(2, keyVal.length);
      Assert.assertEquals(expectedMap.get(keyVal[0]), Integer.valueOf(keyVal[1]));
    }
  }
}
