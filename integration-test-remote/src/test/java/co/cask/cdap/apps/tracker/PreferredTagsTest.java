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

package co.cask.cdap.apps.tracker;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.test.ServiceManager;



import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;

import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.TrackerApp;
import co.cask.tracker.TrackerService;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test various methods of preferred Tags.
 */
public class PreferredTagsTest extends AudiTestBase{
  private static final String TEST_JSON_TAGS = "[\"tag1\",\"tag2\",\"tag3\",\"ta*4\"]";

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestTrackerApp.class);
    RESTClient restClient = getRestClient();
    ProgramClient programClient = getProgramClient();
    ServiceManager TrackService = applicationManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    TrackService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    URL serviceURL = TrackService.getServiceURL();
    URL url = new URL(serviceURL, "v1/tags/promote");
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.post(url).withBody(TEST_JSON_TAGS).build());
    url = new URL(serviceURL, "v1/tags?type=preferred");
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }
}
