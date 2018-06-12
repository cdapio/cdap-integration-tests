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

package co.cask.cdap.operations;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.test.AudiTestBase;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.Map;

/**
 * Tests for operational stats in CDAP.
 */
public class OperationalStatsTest extends AudiTestBase {
  private static final Gson GSON = new Gson();
  private static final Type OPERATIONAL_STATS_LIST_TYPE =
    new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  private static final Type OPERATIONAL_STATS_INDIVIDUAL_PROVIDER_TYPE =
    new TypeToken<Map<String, Map<String, Object>>>() { }.getType();

  @Test
  public void testCDAPStats() throws Exception {
    Map<String, String> cdapInfo = getInfoStats("cdap");
    Assert.assertTrue(Long.parseLong(cdapInfo.get("Uptime")) > 0);
    Map<String, Map<String, Object>> cdapStats = getOtherServiceStats("cdap");
    Map<String, Object> entities = cdapStats.get("entities");
    // Should at least have 1 namespace
    Assert.assertTrue(toInt(entities.get("Namespaces")) >= 1);
    Assert.assertTrue(toInt(entities.get("Artifacts")) >= 0);
    Assert.assertTrue(toInt(entities.get("Applications")) >= 0);
    Assert.assertTrue(toInt(entities.get("Programs")) >= 0);
    Assert.assertTrue(toInt(entities.get("Datasets")) >= 0);
    Assert.assertTrue(toInt(entities.get("Streams")) >= 0);
    Assert.assertTrue(toInt(entities.get("StreamViews")) >= 0);
    Map<String, Object> connections = cdapStats.get("lasthourload");
    Assert.assertTrue(toLong(connections.get("TotalRequests")) >= 0);
    Assert.assertTrue(toLong(connections.get("Successful")) >= 0);
    Assert.assertTrue(toLong(connections.get("ServerErrors")) >= 0);
    Assert.assertTrue(toLong(connections.get("ClientErrors")) >= 0);
    Assert.assertTrue(toLong(connections.get("WarnLogs")) >= 0);
    Assert.assertTrue(toLong(connections.get("ErrorLogs")) >= 0);
    Map<String, Object> transactions = cdapStats.get("transactions");
    Assert.assertTrue(toLong(transactions.get("NumCommittedChangeSets")) >= 0);
    Assert.assertTrue(toLong(transactions.get("NumCommittingChangeSets")) >= 0);
    Assert.assertTrue(toLong(transactions.get("NumInProgressTransactions")) >= 0);
    Assert.assertTrue(toLong(transactions.get("NumInvalidTransactions")) >= 0);
    Assert.assertTrue(toLong(transactions.get("ReadPointer")) >= 0);
    Assert.assertTrue(toLong(transactions.get("WritePointer")) >= 0);
  }
  
  private int toInt(Object object) {
    return ((Number) object).intValue();
  }
  
  private long toLong(Object object) {
    return ((Number) object).longValue();
  }

  private Map<String, String> getInfoStats(String serviceName) throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("system/serviceproviders/");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, clientConfig.getAccessToken());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
    Map<String, Map<String, String>> operationalStats = GSON.fromJson(response.getResponseBodyAsString(),
                                                                      OPERATIONAL_STATS_LIST_TYPE);
    Assert.assertTrue(operationalStats.containsKey(serviceName));
    return operationalStats.get(serviceName);
  }

  private Map<String, Map<String, Object>> getOtherServiceStats(String serviceName) throws Exception {
    RESTClient restClient = getRestClient();
    ClientConfig clientConfig = getClientConfig();
    URL url = clientConfig.resolveURLV3("system/serviceproviders/" + serviceName + "/stats");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, clientConfig.getAccessToken());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), OPERATIONAL_STATS_INDIVIDUAL_PROVIDER_TYPE);
  }
}
