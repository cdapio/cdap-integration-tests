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
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DisruptionTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.chaosmonkey.ChaosMonkeyService;
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
    ChaosMonkeyService chaosMonkeyService = getChaosMonkeyService();
    ApplicationManager applicationManager = deployApplication(ContinuousCounterApp.class);

    FlowManager flowManager = applicationManager.getFlowManager("ContinuousCounterFlow").start();
    flowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    ServiceManager serviceManager = applicationManager.getServiceManager("ContinuousCounterService").start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL, "allCounter");

    // Stopping and restarting CDAP master service with a short delay in between
    chaosMonkeyService.stopAndWait("cdap-master", PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(2);
    chaosMonkeyService.startAndWait("cdap-master", PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Waiting for CDAP to be fully restarted
    checkSystemServices();

    // Query the ContinuousCounterService twice with a short delay in between
    HttpResponse response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    List<Integer> values = GSON.fromJson(response.getResponseBodyAsString(), RETURN_TYPE);
    TimeUnit.SECONDS.sleep(3);
    response = restClient.execute(HttpMethod.GET, url, getClientConfig().getAccessToken());
    List<Integer> newValues = GSON.fromJson(response.getResponseBodyAsString(), RETURN_TYPE);

    // Make sure that the counter has increased, and there are no gaps in the counter
    Assert.assertTrue(newValues.size() > values.size());
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
}
