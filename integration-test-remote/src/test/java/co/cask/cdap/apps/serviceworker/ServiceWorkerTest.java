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
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.apps.TestCoverageUtility;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Test worker that writes to dataset and service that reads from it.
 */
public class ServiceWorkerTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceWorkerTest.class);

  @After
  public void printCoverage() throws Exception {
      // can just printAllHandlerEndpoints just once in a test suite
      getNamespaceClient().create(new NamespaceMeta.Builder().setName("hack").build());
      TestCoverageUtility testCoverageUtility = new TestCoverageUtility();
      testCoverageUtility.getAllHandlerEndpoints();
      testCoverageUtility.printAllHandlerEndpoints();
      testCoverageUtility.printCoveredEndpoints(ServiceWorkerTest.class.getName());
  }

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ApplicationManager applicationManager = deployApplication(ServiceApplication.class);

    ServiceManager serviceManager = applicationManager.getServiceManager(ServiceApplication.SERVICE_NAME).start();
    serviceManager.waitForStatus(true, 60, 1);


    // TODO: better way to wait for service to be up.
    TimeUnit.SECONDS.sleep(60);
    URL serviceURL = serviceManager.getServiceURL();
    URL url = new URL(serviceURL, "read/" + DatasetWorker.WORKER_DATASET_TEST_KEY);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());

    // hit the service endpoint, get for worker_key, should return 204 (null)
    Assert.assertEquals(204, response.getResponseCode());

    // start the worker
    WorkerManager workerManager = applicationManager.getWorkerManager(ServiceApplication.WORKER_NAME).start();
    workerManager.waitForStatus(true, 60, 1);
    TimeUnit.SECONDS.sleep(10);

    // check if the worker's write to the table was successful
    waitForWorkerResult(url, restClient, 1, 60, TimeUnit.SECONDS);

    // todo : this shouldn't be necessary as the worker exits after writing to table
    TimeUnit.SECONDS.sleep(10);
    workerManager.stop();
    workerManager.waitForStatus(false, 60, 1);

    // try starting the service , while its running, should throw IllegalArgumentException
    boolean alreadyRunning = false;
    try {
      serviceManager.start();
    } catch (Exception e) {
      alreadyRunning = (e instanceof IllegalStateException);
    }
    Assert.assertTrue(alreadyRunning);

    serviceManager.stop();
    serviceManager.waitForStatus(false, 60, 1);
  }

  protected void waitForWorkerResult(URL url, RESTClient restClient, long sleepTime, long timeout,
                                     TimeUnit timeoutUnit) throws Exception {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    long startTime = System.currentTimeMillis();
    boolean statusMatched = (response.getResponseCode() == 200);
    while (!statusMatched && (System.currentTimeMillis() - startTime) < timeoutMillis) {
      timeoutUnit.sleep(sleepTime);
      response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
      statusMatched = (response.getResponseCode() == 200);
    }

    if (!statusMatched) {
      throw new IllegalStateException("Program state not as expected. Expected " + 200);
    }
    // check value returned
    Assert.assertEquals("\"" + DatasetWorker.WORKER_DATASET_TEST_VALUE + "\"",
                        Bytes.toString(response.getResponseBody()));
  }

}
