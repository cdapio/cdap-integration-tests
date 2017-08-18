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

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.PluginClassDeserializer;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.amazonaws.util.Throwables;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test worker that writes to dataset and service that reads from it.
 */
public class ServiceWorkerTest extends AudiTestBase {
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();
  private static final ArtifactId artifactId = TEST_NAMESPACE.artifact("image-app", "1.0.0");
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();

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
    // worker will stop automatically
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // check if the worker's write to the table was successful
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("\"" + DatasetWorker.WORKER_DATASET_TEST_VALUE + "\"",
                        Bytes.toString(response.getResponseBody()));

    // try starting the service, while its running, should throw exception
    try {
      serviceManager.start();
      Assert.fail();
    } catch (Throwable expected) {
      expected = Throwables.getRootCause(expected);
      Assert.assertTrue(expected.getMessage().startsWith("409: "));
      Assert.assertTrue(expected.getMessage().contains(ServiceApplication.SERVICE_NAME + " is already running"));
    }

    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Now testing the artifact listing / class loading using the Artifact HTTP Service
    final File directiveJar =
      new File(ServiceWorkerTest.class.getClassLoader().getResource("image-directives.jar").toURI());

    ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());

    Set<ArtifactRange> parentArtifacts = new HashSet<>();
    parentArtifacts.add(new ArtifactRange(NamespaceId.SYSTEM.getNamespace(),
                                         "cdap-data-pipeline",
                                         new ArtifactVersion("4.3.0-SNAPSHOT"),
                                         new ArtifactVersion("10.0.0-SNAPSHOT")));

    parentArtifacts.add(new ArtifactRange(NamespaceId.SYSTEM.getNamespace(),
                                         "cdap-data-streams",
                                         new ArtifactVersion("4.3.0-SNAPSHOT"),
                                         new ArtifactVersion("10.0.0-SNAPSHOT")));
    artifactClient.add(artifactId, parentArtifacts, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return new FileInputStream(directiveJar);
      }
    });

    serviceManager = applicationManager.getServiceManager(ServiceApplication.ARTIFACT_SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);


    serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    url = new URL(serviceURL, "list");

    // make sure we are able to list artifacts and its not empty
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    List<ArtifactInfo> artifactInfoList = GSON.fromJson(response.getResponseBodyAsString(), ARTIFACT_INFO_LIST_TYPE);
    Assert.assertTrue(artifactInfoList.size() > 0);
    // try to load class ImageMetadataReader from the jar metadata-extractor-2.9.1.jar located in the lib folder
    url = new URL(serviceURL, "test-artifact/image-app/load/com.drew.imaging.ImageMetadataReader");
    response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    serviceManager.stop();
    serviceManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
