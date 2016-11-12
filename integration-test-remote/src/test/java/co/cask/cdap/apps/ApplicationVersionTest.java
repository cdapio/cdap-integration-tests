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

package co.cask.cdap.apps;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Tests creating Application with version
 */
public class ApplicationVersionTest extends AudiTestBase {
  private static final ArtifactId artifactId = TEST_NAMESPACE_ENTITY.artifact("cfg-app", "1.0.0");
  private static final int MAX_NUM_CALLS = 50;

  private ServiceClient serviceClient;

  @Before
  public void uploadArtifact() throws Exception {
    // Upload app artifact
    addAppArtifact(artifactId, ConfigTestApp.class);
    serviceClient = new ServiceClient(getClientConfig(), getRestClient());
  }

  @Test
  public void testAppVersionsCreationUpdateDeletion() throws Exception {
    // Deploy ConfigTestApp v1
    ApplicationId appId1 = TEST_NAMESPACE_ENTITY.app("ConfigTestApp", "v1-SNAPSHOT");
    AppRequest<ConfigTestApp.ConfigClass> createRequestV1 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS1", "tD1", "tV1"));
    ApplicationManager appManagerV1 = deployApplication(appId1, createRequestV1);

    // Start the service in ConfigTestApp v1
    ServiceManager serviceManagerV1 = appManagerV1.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV1.start();
    serviceManagerV1.waitForStatus(true, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);

    // Verify the service in ConfigTestApp v1 returns correct responses
    URL urlV1 = new URL(serviceManagerV1.getServiceURL(), "ping");
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
    } catch (IllegalStateException expected) {
      Assert.assertTrue(expected.getMessage().contains("simpleService is already running"));
    }

    // Start the service after stopping the original service and verify that updated response is returned
    serviceManagerV1.stop();
    serviceManagerV1.waitForStatus(false, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);
    serviceManagerV1.start();
    serviceManagerV1.waitForStatus(true, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);
    response = getRestClient().execute(HttpRequest.get(urlV1).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1_update", response.getResponseBodyAsString());

    // Deploy ConfigTestApp v2
    ApplicationId appId2 = TEST_NAMESPACE_ENTITY.app("ConfigTestApp", "v2-SNAPSHOT");
    AppRequest<ConfigTestApp.ConfigClass> createRequestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS2", "tD2", "tV2"));
    ApplicationManager appManagerV2 = deployApplication(appId2, createRequestV2);

    // Start the service in ConfigTestApp v2
    ServiceManager serviceManagerV2 = appManagerV2.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV2.start();
    serviceManagerV2.waitForStatus(true, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);

    // Verify that the services in ConfigTestApp v1 and v2 are running concurrently and returning correct response
    response = getRestClient().execute(HttpRequest.get(urlV1).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1_update", response.getResponseBodyAsString());
    URL urlV2 = new URL(serviceManagerV2.getServiceURL(), "ping");
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
    serviceManagerV1.waitForStatus(false, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);
    serviceManagerV2.stop();
    serviceManagerV2.waitForStatus(false, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);
  }

  @Test
  public void testRouteConfig() throws Exception {
    // Deploy ConfigTestApp v1
    final String version1 = "v1";
    ApplicationId appId1 = TEST_NAMESPACE_ENTITY.app(ConfigTestApp.NAME, version1);
    AppRequest<ConfigTestApp.ConfigClass> createRequestV1 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS1", "tD1", "tV1"));
    ApplicationManager appManagerV1 = deployApplication(appId1, createRequestV1);

    // Start the service in ConfigTestApp v1
    ServiceManager serviceManagerV1 = appManagerV1.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV1.start();
    serviceManagerV1.waitForStatus(true, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);

    // Verify that calling non-versioned service endpoint of ping method is routed to ConfigTestApp v1
    String pingMethod = "ping";
    HttpResponse response = serviceClient.callServiceMethod(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                            ConfigTestApp.SERVICE_NAME, pingMethod);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV1", response.getResponseBodyAsString());

    // Verify that getting empty RouteConfig initially
    Assert.assertTrue(serviceClient.getRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                   ConfigTestApp.SERVICE_NAME).isEmpty());

    // Setting RouteConfig with non-existing version will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 100,
                                            "NONEXISTING", 0), "NONEXISTING");
    // Setting RouteConfig with total percentage not equal to 100 will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 99), "Percentage");

    // Deploy ConfigTestApp v2
    final String version2 = "v2";
    ApplicationId appId2 = TEST_NAMESPACE_ENTITY.app(ConfigTestApp.NAME, version2);
    AppRequest<ConfigTestApp.ConfigClass> createRequestV2 = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
      new ConfigTestApp.ConfigClass("tS2", "tD2", "tV2"));
    ApplicationManager appManagerV2 = deployApplication(appId2, createRequestV2);

    // Start the service in ConfigTestApp v2
    ServiceManager serviceManagerV2 = appManagerV2.getServiceManager(ConfigTestApp.SERVICE_NAME);
    serviceManagerV2.start();
    serviceManagerV2.waitForStatus(true, 80, 1);

    URL urlV2 = new URL(serviceManagerV2.getServiceURL(), "ping");
    response = getRestClient().execute(HttpRequest.get(urlV2).build(), getClientConfig().getAccessToken(),
                                       HttpURLConnection.HTTP_OK);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("tV2", response.getResponseBodyAsString());

    // Verify that both service v1 and v2 can be reached with the default random routing strategy
    assureBothVersionsReached(pingMethod);

    // Setting RouteConfig with total percentage not equal to 100 will fail
    storeInvalidRouteConfig(ImmutableMap.of(version1, 100, version2, 1), "Percentage");
    storeInvalidRouteConfig(ImmutableMap.of(version1, 98, version2, 1), "Percentage");

    // Route all traffic to v1
    serviceClient.storeRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME, ConfigTestApp.SERVICE_NAME,
                                   ImmutableMap.of(version1, 100, version2, 0));
    for (int i = 0; i < 20; i++) {
      response = serviceClient.callServiceMethod(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                 ConfigTestApp.SERVICE_NAME, pingMethod);
      if ("tV2".equals(response.getResponseBodyAsString())) {
        Assert.fail("All traffic should be routed to service v1 but service v2 is also reached.");
      }
    }

    // Route all traffic to v2
    serviceClient.storeRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME, ConfigTestApp.SERVICE_NAME,
                                   ImmutableMap.of(version1, 0, version2, 100));
    for (int i = 0; i < 20; i++) {
      response = serviceClient.callServiceMethod(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                 ConfigTestApp.SERVICE_NAME, pingMethod);
      if ("tV1".equals(response.getResponseBodyAsString())) {
        Assert.fail("All traffic should be routed to service v2 but service v1 is also reached.");
      }
    }

    // Storing and getting RouteConfig with total percentage equal to 100 will succeed
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 10, version2, 90));
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 20, version2, 80));
    storeAndGetValidRouteConfig(ImmutableMap.of(version1, 60, version2, 40));

    // Delete RouteConfig and verify that both service v1 and v2 can be reached after deletion
    serviceClient.deleteRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME, ConfigTestApp.SERVICE_NAME);
    assureBothVersionsReached(pingMethod);

    // Cannot delete the namespace because service v1 and v2 are running
    NamespaceClient namespaceClient = getNamespaceClient();
    try {
      namespaceClient.delete(TEST_NAMESPACE_ENTITY);
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("Some programs are currently running"));
    }

    // Stop service v1
    serviceManagerV1.stop();
    serviceManagerV1.waitForStatus(false, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);

    // Cannot delete the namespace because service v2 is running
    try {
      namespaceClient.delete(TEST_NAMESPACE_ENTITY);
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("Some programs are currently running"));
    }

    // Stop service v2 and delete the namespace successfully
    serviceManagerV2.stop();
    serviceManagerV2.waitForStatus(false, 1, PROGRAM_START_STOP_TIMEOUT_SECONDS);
  }

  private void storeInvalidRouteConfig(Map<String, Integer> routeConfig, String expectedMsg) throws Exception {
    try {
      serviceClient.storeRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME, ConfigTestApp.SERVICE_NAME,
                                     routeConfig);
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(expectedMsg));
    }
  }

  private void storeAndGetValidRouteConfig(Map<String, Integer> routeConfig) throws Exception {
    serviceClient.storeRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME, ConfigTestApp.SERVICE_NAME, routeConfig);
    Assert.assertEquals(routeConfig, serviceClient.getRouteConfig(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                                  ConfigTestApp.SERVICE_NAME));
  }

  private void assureBothVersionsReached(String pingMethod) throws
    UnauthorizedException, IOException, UnauthenticatedException {
    boolean v1Chosen = false;
    boolean v2Chosen = false;
    int i = 0;
    while (i < MAX_NUM_CALLS) {
      HttpResponse response = serviceClient.callServiceMethod(TEST_NAMESPACE_ENTITY, ConfigTestApp.NAME,
                                                              ConfigTestApp.SERVICE_NAME, pingMethod);
      String responseString = response.getResponseBodyAsString();
      v1Chosen = v1Chosen || "tV1".equals(responseString);
      v2Chosen = v2Chosen || "tV2".equals(responseString);
      if (v1Chosen && v2Chosen) {
        return;
      }
      i++;
    }
    Assert.fail();
  }
}
