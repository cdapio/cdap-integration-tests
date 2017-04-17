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

package co.cask.cdap.app.resiliency;

import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DisruptionTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.chaosmonkey.proto.ClusterDisrupter;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ContinuousCounterTest extends DisruptionTestBase {
  private static final Gson GSON = new Gson();
  private static final Type RETURN_TYPE = new TypeToken<List<Integer>>() { }.getType();

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ClusterDisrupter clusterDisrupter = getClusterDisrupter();
    ApplicationManager applicationManager = deployApplication(ContinuousCounterApp.class);

    FlowManager flowManager = applicationManager.getFlowManager("ContinuousCounterFlow").start();
    flowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    ServiceManager serviceManager = applicationManager.getServiceManager("ContinuousCounterService").start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL, "allCounter");

    // Stopping and restarting CDAP master service
    clusterDisrupter.stopAndWait("cdap-master", PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    clusterDisrupter.startAndWait("cdap-master", PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Waiting for CDAP to be fully restarted
    checkSystemServices();

    // Query the ContinuousCounterService twice with a short delay in between
    HttpResponse response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    List<Integer> values = GSON.fromJson(response.getResponseBodyAsString(), RETURN_TYPE);

    // Check that flow has restarted and continues running
    CheckCounter checkCounter = new CheckCounter(values.size(), restClient, url, getClientConfig().getAccessToken());
    Tasks.waitFor(true, checkCounter, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    // Check that flow restarted properly and there are no gaps in the counter
    response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    List<Integer> newValues = GSON.fromJson(response.getResponseBodyAsString(), RETURN_TYPE);
    Assert.assertFalse(hasGaps(newValues));
  }

  private boolean hasGaps(List<Integer> values) {
    Collections.sort(values);
    for (int idx = 0; idx < values.size() - 1; ++idx) {
      if (values.get(idx) != values.get(idx + 1) - 1) {
        return true;
      }
    }
    return false;
  }

  public static class CheckCounter implements Callable<Boolean> {
    private final Integer initialValue;
    private final RESTClient restClient;
    private final URL url;
    private final AccessToken accessToken;

    CheckCounter(Integer initialValue, RESTClient restClient, URL url, AccessToken accessToken) {
      this.initialValue = initialValue;
      this.restClient = restClient;
      this.url = url;
      this.accessToken = accessToken;
    }

    public Boolean call() throws Exception {
      HttpResponse response = restClient.execute(HttpMethod.GET, url, accessToken);
      List<Integer> values = GSON.fromJson(response.getResponseBodyAsString(), RETURN_TYPE);
      return values.size() > initialValue;
    }
  }
}
