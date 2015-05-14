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

import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private static RESTClient REST_CLIENT;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    HttpResponse response = new RESTClient(getClientConfig()).execute(HttpMethod.GET, getClientConfig().resolveURLV3("config/cdap"), getClientConfig().getAccessToken());
    boolean allOk = false;
    for (int i = 0; i < 10; i++) {
      if (allOk = allOk()) {
        break;
      }
    }
    Assert.assertTrue("Expected all system services to be OK.", allOk);
    JsonArray jsonArray = new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonArray();
    boolean resetEnabled = Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET;
    for (JsonElement jsonElement : jsonArray) {
      if (Constants.Dangerous.UNRECOVERABLE_RESET.equals(jsonElement.getAsJsonObject().get("name").getAsString())) {
        resetEnabled = jsonElement.getAsJsonObject().get("value").getAsBoolean();
        break;
      }
    }
    Preconditions.checkState(resetEnabled, "UnrecoverableReset not enabled.");

    REST_CLIENT = new RESTClient(getClientConfig());
    REST_CLIENT.addListener(new RESTClient.Listener() {
      @Override
      public void onRequest(HttpRequest httpRequest, int i) {
        String body = null;
        try {
          InputSupplier<? extends InputStream> inputSupplier = httpRequest.getBody();
          if (inputSupplier != null) {
            body = IOUtils.toString(inputSupplier.getInput());
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        LOG.info("Making request: {} {} - Body: {}", httpRequest.getMethod(), httpRequest.getURL(), body);
      }

      @Override
      public void onResponse(HttpRequest httpRequest, HttpResponse httpResponse, int i) {
        LOG.info("Received response: [{}] {}", httpResponse.getResponseCode(), httpResponse.getResponseBodyAsString());
      }
    });
  }

  private boolean allOk() throws Exception {
    Map<String, String> allSystemServiceStatus = new MonitorClient(getClientConfig()).getAllSystemServiceStatus();
    LOG.info(allSystemServiceStatus.toString());
    for (String status : allSystemServiceStatus.values()) {
      if (!"OK".equals(status)) {
        return false;
      }
    }
    return true;
  }

  // should always use this RESTClient because it has listeners which log upon requests made and responses received.
  protected RESTClient getRestClient() {
    Preconditions.checkNotNull(REST_CLIENT, "RESTClient not yet initialized.");
    return REST_CLIENT;
  }
}
