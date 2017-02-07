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

package co.cask.cdap.apps.serviceworker;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test worker that writes to dataset and service that reads from it.
 */
public class ServiceWorkerTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ApplicationManager applicationManager = deployApplication(ServiceApplication.class);

    ServiceManager serviceManager = applicationManager.getServiceManager(ServiceApplication.SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);


    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL url = new URL(serviceURL, "read/" + DatasetWorker.WORKER_DATASET_TEST_KEY);

    // hit the service endpoint, get for worker_key, should return 204 (null)
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getResponseCode());

    // start the worker
    WorkerManager workerManager = applicationManager.getWorkerManager(ServiceApplication.WORKER_NAME).start();
    workerManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // worker will stop automatically
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // check if the worker's write to the table was successful
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("\"" + DatasetWorker.WORKER_DATASET_TEST_VALUE + "\"",
                        Bytes.toString(response.getResponseBody()));

    // try starting the service , while its running, should throw IllegalArgumentException
    boolean alreadyRunning = false;
    try {
      serviceManager.start();
    } catch (Exception e) {
      alreadyRunning = (e instanceof IllegalStateException);
    }
    Assert.assertTrue(alreadyRunning);

    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.COMPLETED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
