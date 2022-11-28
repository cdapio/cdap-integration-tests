/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.mapreduce.readless;

import com.google.gson.Gson;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.suite.category.SDKIncompatible;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Category({
  // this is a flaky test when run against SDK, because of CDAP-7227
  SDKIncompatible.class
})
public class ReadlessIncrementTest extends AudiTestBase {

  // This uses a MapReduce that counts the lines in a mocked input. It writes three values to an output table:
  // - one is incremented for each line that a mapper sees
  // - one is incremented from the cleanup() of each mapper with the total number of lines it has seen
  // - one is written by the reducer after it has summed up all the counts
  // The three values must be the same.

  @Test
  public void testReadlessIncrementsInMapReduce()
    throws Exception {

    ApplicationManager appManager = deployApplication(ReadlessApp.class);

    ServiceManager serviceManager = appManager.getServiceManager(ReadlessApp.SERVICE_NAME);
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);

    MapReduceManager mapReduceManager = appManager.getMapReduceManager(ReadlessApp.MAPREDUCE_NAME);
    startAndWaitForRun(mapReduceManager, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    URL url = new URL(serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS), "get");
    HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken(),
                                                    HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Map<String, Long> map = new Gson().fromJson(response.getResponseBodyAsString(),
                                                new TypeToken<Map<String, Long>>() {
                                                }.getType());
    Assert.assertEquals(new Long(20L), map.get("increments"));
    Assert.assertEquals(new Long(20L), map.get("mapCount"));
    Assert.assertEquals(new Long(20L), map.get("reduceCount"));
  }
}
