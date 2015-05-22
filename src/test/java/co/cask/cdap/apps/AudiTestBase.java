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
import java.util.concurrent.TimeUnit;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private static final String CLUSTER_IP_FILE_NAME = "cdap-auto-ip.txt";
  private static RESTClient REST_CLIENT;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    boolean allOk = false;
    for (int i = 0; i < 10; i++) {
      if (allOk = allOk()) {
        break;
      }
      TimeUnit.SECONDS.sleep(5);
    }
    Assert.assertTrue("Expected all system services to be OK.", allOk);

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

  /*
  TODO: uncomment when the functionality is available in IntegrationTestBase
  @Override
  protected String getInstanceURI() {
    File file = new File(CLUSTER_IP_FILE_NAME);
    if (!file.exists()) {
      LOG.warn("File does not exist: {}", file.getAbsolutePath());
      return super.getInstanceURI();
    }
    try {
      FileReader fileReader = new FileReader(file);
      String clusterIp = IOUtils.toString(fileReader);
      clusterIp = clusterIp.replaceAll("\n", "");
      LOG.info("clusterIp: {}", clusterIp);
      return clusterIp;
    } catch (IOException ioe) {
      LOG.error("Failed to get clusterIp from file.", ioe);
      return super.getInstanceURI();
    }
  }*/


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
    if (REST_CLIENT == null) {
      REST_CLIENT = constructRestClient();
    }
    return REST_CLIENT;
  }

  // constructs a RestClient with logging upon each request
  private RESTClient constructRestClient() {
    RESTClient restClient = new RESTClient(getClientConfig());
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
    return restClient;
  }
}
