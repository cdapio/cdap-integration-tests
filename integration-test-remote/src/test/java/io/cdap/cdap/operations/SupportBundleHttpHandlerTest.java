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

import com.google.gson.Gson;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.Map;

/**
 * Tests for support bundle in CDAP.
 */
public class SupportBundleHttpHandlerTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  @Test
  public void testCreateSupportBundle() throws Exception {
//    NamespaceClient namespaceClient = getNamespaceClient();
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("support/bundle");
    HttpResponse response = restClient.execute(HttpMethod.POST, url, clientConfig.getAccessToken());
    Assert.assertEquals(HttpResponseStatus.CREATED.getCode(), response.getResponseCode());
    Assert.assertNotNull(response.getResponseBodyAsString());
  }
}
