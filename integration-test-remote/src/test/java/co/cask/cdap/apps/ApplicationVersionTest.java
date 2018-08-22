/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.amazonaws.util.Throwables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Tests creating Application with version
 */
public class ApplicationVersionTest extends AudiTestBase {
  private static final ArtifactId artifactId = TEST_NAMESPACE.artifact("cfg-app", "1.0.0");

  @Before
  public void uploadArtifact() throws Exception {
    // Upload app artifact
    addAppArtifact(artifactId, ConfigTestApp.class);
  }

  @Test
  public void testAppVersionsCreationUpdateDeletion() throws Exception {
    // Deploy ConfigTestApp v1
    ApplicationId appId1 = TEST_NAMESPACE.app("ConfigTestApp", "v1-SNAPSHOT");
    AppRequest<ConfigTestApp.ConfigClass> createRequestV1 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS1", "tD1", "tV1"));
    ApplicationManager appManagerV1 = deployApplication(appId1, createRequestV1);

    // Start the service in ConfigTestApp v1
    ServiceManager serviceManagerV1 = appManagerV1.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV1.start();
    serviceManagerV1.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Verify the service in ConfigTestApp v1 returns correct responses
    URL urlV1 = new URL(serviceManagerV1.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS), "ping");
    // we have to make the first handler call after service starts with a retry
    HttpResponse response = getRestClient().execute(HttpRequest.get(urlV1).build(), getClientConfig().getAccessToken(),
                                                    HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1", response.getResponseBodyAsString());

    // Can update ConfigTestApp v1 because its version has suffix "-SNAPSHOT"
    AppRequest<ConfigTestApp.ConfigClass> createRequestV1Update = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS1", "tD1", "tV1_update"));
    deployApplication(appId1, createRequestV1Update);

    // Start the service after updating ConfigTestApp v1 will fail because the same service is running
    try {
      serviceManagerV1.start();
      Assert.fail();
    } catch (Throwable expected) {
      expected = Throwables.getRootCause(expected);
      Assert.assertTrue(expected.getMessage().startsWith("409: "));
      Assert.assertTrue(expected.getMessage().contains(ConfigTestApp.SERVICE_NAME + " is already running"));
    }

    // Start the service after stopping the original service and verify that updated response is returned
    serviceManagerV1.stop();
    serviceManagerV1.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    serviceManagerV1.start();
    serviceManagerV1.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    response = getRestClient().execute(HttpRequest.get(urlV1).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1_update", response.getResponseBodyAsString());

    // Deploy ConfigTestApp v2
    ApplicationId appId2 = TEST_NAMESPACE.app("ConfigTestApp", "v2-SNAPSHOT");
    AppRequest<ConfigTestApp.ConfigClass> createRequestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS2", "tD2", "tV2"));
    ApplicationManager appManagerV2 = deployApplication(appId2, createRequestV2);

    // Start the service in ConfigTestApp v2
    ServiceManager serviceManagerV2 = appManagerV2.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV2.start();
    serviceManagerV2.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Verify that the services in ConfigTestApp v1 and v2 are running concurrently and returning correct response
    response = getRestClient().execute(HttpRequest.get(urlV1).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1_update", response.getResponseBodyAsString());
    URL urlV2 = new URL(serviceManagerV2.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS), "ping");
    response = getRestClient().execute(HttpRequest.get(urlV2).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV2", response.getResponseBodyAsString());

    // Delete ConfigTestApp v1 should fail because the service is still running
    try {
      appManagerV1.delete();
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().startsWith("409"));
    }

    // stop both services
    serviceManagerV1.stop();
    serviceManagerV1.waitForRuns(ProgramRunStatus.KILLED, 2, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    serviceManagerV2.stop();
    serviceManagerV2.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
