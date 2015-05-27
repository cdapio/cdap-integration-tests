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

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
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
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private RESTClient restClient;

  @Before
  @Override
  public void setUp() throws Exception {
    // we need to setup the RestClient before anything happens even in IntegrationTestBase, so that any requests
    // made can go through our RestClient
    setupRestClient();

    super.setUp();

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return allOk();
      }
    }, 20, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    assertUnrecoverableResetEnabled();
  }

  private void assertUnrecoverableResetEnabled() throws IOException, UnauthorizedException {
    // TODO: use MetaClient#getCDAPConfig, once the method is available
    HttpResponse response = getRestClient().execute(HttpMethod.GET, getClientConfig().resolveURLV3("config/cdap"),
                                                    getClientConfig().getAccessToken());
    JsonArray jsonArray = new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonArray();
    boolean resetEnabled = Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET;
    for (JsonElement jsonElement : jsonArray) {
      if (Constants.Dangerous.UNRECOVERABLE_RESET.equals(jsonElement.getAsJsonObject().get("name").getAsString())) {
        resetEnabled = jsonElement.getAsJsonObject().get("value").getAsBoolean();
        break;
      }
    }
    Preconditions.checkState(resetEnabled, "UnrecoverableReset not enabled.");
  }

  private boolean allOk() throws Exception {
    // TODO: use MonitorClient#allSystemServicesOk, once the method is available
    Map<String, String> allSystemServiceStatus = new MonitorClient(getClientConfig()).getAllSystemServiceStatus();
    LOG.info(allSystemServiceStatus.toString());
    for (String status : allSystemServiceStatus.values()) {
      if (!"OK".equals(status)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig(), getRestClient(), getApplicationClient());
  }

  @Override
  protected ApplicationClient getApplicationClient() {
    return new ApplicationClient(getClientConfig(), getRestClient());
  }

  // should always use this RESTClient because it has listeners which log upon requests made and responses received.
  protected RESTClient getRestClient() {
    Preconditions.checkNotNull(restClient, "RestClient not yet initialized.");
    return restClient;
  }

  // constructs a RestClient with logging upon each request
  private void setupRestClient() {
    restClient = new RESTClient(getClientConfig());
    restClient.addListener(new RESTClient.Listener() {
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
        LOG.info("Received response: [{}] Response Body: {}",
                 httpResponse.getResponseCode(), httpResponse.getResponseBodyAsString());
      }
    });
  }
}
