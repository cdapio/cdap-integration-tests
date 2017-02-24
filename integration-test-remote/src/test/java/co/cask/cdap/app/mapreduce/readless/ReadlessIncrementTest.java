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

package co.cask.cdap.app.mapreduce.readless;

import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.suite.category.SDKIncompatible;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    throws IOException, InterruptedException, TimeoutException, UnauthenticatedException,
    UnauthorizedException, ExecutionException {

    ApplicationManager appManager = deployApplication(ReadlessApp.class);

    try {
      ServiceManager serviceManager = appManager.getServiceManager(ReadlessApp.SERVICE_NAME);
      serviceManager.start();

      MapReduceManager mapReduceManager = appManager.getMapReduceManager(ReadlessApp.MAPREDUCE_NAME);
      mapReduceManager.start();
      mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

      serviceManager.waitForRun(ProgramRunStatus.RUNNING, 5, TimeUnit.SECONDS);
      URL url = new URL(serviceManager.getServiceURL(), "get");
      HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken(),
                                                      HttpURLConnection.HTTP_OK);
      Assert.assertEquals(200, response.getResponseCode());
      Map<String, Long> map = new Gson().fromJson(response.getResponseBodyAsString(),
                                                  new TypeToken<Map<String, Long>>() {
                                                  }.getType());
      Assert.assertEquals(new Long(20L), map.get("increments"));
      Assert.assertEquals(new Long(20L), map.get("mapCount"));
      Assert.assertEquals(new Long(20L), map.get("reduceCount"));
    } finally {
      appManager.stopAll();
    }
  }
}
