/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.operations;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.lib.SupportBundleFile;
import io.cdap.cdap.lib.SupportBundleFiles;
import io.cdap.cdap.lib.SupportBundleOperationStatus;
import io.cdap.cdap.lib.SupportBundlePipelineStatus;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for support bundle in CDAP.
 */
public class SupportBundleHttpHandlerTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  private static final NamespaceId NS1 = new NamespaceId("ns1");

  private static final NamespaceMeta ns1Meta = new NamespaceMeta.Builder().setName(NS1)
    .setDescription("testDescription")
    .setSchedulerQueueName("testSchedulerQueueName")
    .build();

  @Before
  public void setUp() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    registerForDeletion(NS1);
    if (!namespaceClient.exists(NS1)) {
      namespaceClient.create(ns1Meta);
    }
  }

  @After
  public void teardown() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    namespaceClient.delete(NS1);
  }

  @Test
  public void testCreateSupportBundle() throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("support/bundle");
    HttpResponse response = restClient.execute(HttpMethod.POST, url, clientConfig.getAccessToken());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
    Assert.assertNotNull(response.getResponseBodyAsString());
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("support/bundle?namespace=ns1");
    HttpResponse response = restClient.execute(HttpMethod.POST, url, clientConfig.getAccessToken());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
    Assert.assertNotNull(response.getResponseBodyAsString());
  }

  //TODO Enable this test after merging the get support bundle endpoints
//  @Test
//  public void testGetSupportBundleFilesWithValidNamespace() throws Exception {
//    RESTClient restClient = getRestClient();
//    ClientConfig clientConfig = getClientConfig();
//    URL url = clientConfig.resolveURLV3("support/bundle?namespace=ns1");
//    HttpResponse response = restClient.execute(HttpMethod.POST, url, clientConfig.getAccessToken());
//    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
//    Assert.assertNotNull(response.getResponseBodyAsString());
//    String uuid = response.getResponseBodyAsString();
//
//    URL getUrl = clientConfig.resolveURLV3(String.format("support/bundle/%s", uuid));
//    Tasks.waitFor(true, () -> {
//      HttpResponse getResponse = restClient.execute(HttpMethod.GET, getUrl, clientConfig.getAccessToken());
//      if (getResponse.getResponseCode() != HttpResponseStatus.OK.getCode()) {
//        return false;
//      }
//      List<SupportBundleOperationStatus> supportBundleOperationStatusList =
//        GSON.fromJson(getResponse.getResponseBodyAsString(), new TypeToken<List<SupportBundleOperationStatus>>() {
//        }.getType());
//      for (SupportBundleOperationStatus supportBundleOperationStatus : supportBundleOperationStatusList) {
//        if (supportBundleOperationStatus.getBundleStatus() != CollectionState.FINISHED) {
//          return false;
//        }
//        SupportBundlePipelineStatus supportBundlePipelineStatus =
//          supportBundleOperationStatus.getSupportBundlePipelineStatus();
//        CollectionState systemLogTaskStatus = supportBundlePipelineStatus.getSystemLogTaskStatus();
//        CollectionState runtimeInfoTaskStatus = supportBundlePipelineStatus.getRuntimeInfoTaskStatus();
//        CollectionState runtimeLogTaskStatus = supportBundlePipelineStatus.getRuntimeLogTaskStatus();
//        if ((systemLogTaskStatus != null && systemLogTaskStatus != CollectionState.FINISHED) ||
//          (runtimeInfoTaskStatus != null && runtimeInfoTaskStatus != CollectionState.FINISHED) ||
//          (runtimeLogTaskStatus != null && runtimeLogTaskStatus != CollectionState.FINISHED)) {
//          return false;
//        }
//      }
//      return true;
//    }, 60, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
//    URL getFilesUrl = clientConfig.resolveURLV3(String.format("support/bundle/%s/files", uuid));
//
//    HttpResponse getFilesResponse = restClient.execute(HttpMethod.GET, getFilesUrl, clientConfig.getAccessToken());
//    Assert.assertEquals(HttpResponseStatus.OK.getCode(), getFilesResponse.getResponseCode());
//    Assert.assertNotNull(getFilesResponse.getResponseBodyAsString());
//    List<SupportBundleFiles> supportBundleFilesList =
//      GSON.fromJson(getFilesResponse.getResponseBodyAsString(), new TypeToken<List<SupportBundleFiles>>() {
//      }.getType());
//    for (SupportBundleFiles supportBundleFiles : supportBundleFilesList) {
//      List<SupportBundleFile> supportBundleFileList = supportBundleFiles.getSupportBundleFileList();
//      if (supportBundleFiles.getApplicationName().equals("system-log")) {
//        Assert.assertEquals(10, supportBundleFileList.size());
//      }
//    }
//
//  }

  @Test
  public void testCreateSupportBundleWithInvalidNamespace() throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("support/bundle?namespace=ns2");
    try {
      restClient.execute(HttpMethod.POST, url, clientConfig.getAccessToken());
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (Exception expected) {
      // expected
    }
  }

  @Test
  public void testSupportBundleHealthCheck() throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("health/health.check.appfabric.service");
    try {
      HttpResponse response = restClient.execute(HttpMethod.GET, url, clientConfig.getAccessToken());
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
      Assert.assertNotNull(response.getResponseBodyAsString());
      JsonObject healthResponse = GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class);
      Assert.assertTrue(healthResponse.has("value"));
      Assert.assertTrue(healthResponse.getAsJsonObject("value").has("heapDump"));
      Assert.assertTrue(healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").has("freeMemory"));
      Assert.assertNotNull(
        healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").get("freeMemory").getAsString());

      Assert.assertTrue(healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").has("totalMemory"));
      Assert.assertNotNull(
        healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").get("totalMemory").getAsString());

      Assert.assertTrue(healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").has("usedMemory"));
      Assert.assertNotNull(
        healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").get("usedMemory").getAsString());

      Assert.assertTrue(healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").has("maxMemory"));
      Assert.assertNotNull(
        healthResponse.getAsJsonObject("value").getAsJsonObject("heapDump").get("maxMemory").getAsString());

      Assert.assertTrue(healthResponse.getAsJsonObject("value").has("threadDump"));
      Assert.assertNotNull(healthResponse.getAsJsonObject("value").get("threadDump"));
    } catch (Exception expected) {
      // expected
    }
  }
}
