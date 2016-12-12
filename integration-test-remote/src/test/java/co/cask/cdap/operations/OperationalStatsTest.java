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

package co.cask.cdap.operations;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
  @Category(MapR5Incompatible.class)
  // incompatible with MapR right now because a lot of these stats are currently unavailable for MapRFS
  public void testHDFSStats() throws Exception {
    Map<String, String> hdfsInfo = getInfoStats("hdfs");
    Assert.assertTrue(hdfsInfo.containsKey("Version") && !hdfsInfo.get("Version").isEmpty());
    Assert.assertTrue(hdfsInfo.containsKey("WebURL") && !hdfsInfo.get("WebURL").isEmpty());
    Assert.assertTrue(hdfsInfo.containsKey("LogsURL") && !hdfsInfo.get("LogsURL").isEmpty());
    Map<String, Map<String, Object>> hdfsStats = getOtherServiceStats("hdfs");
    Assert.assertTrue(hdfsStats.containsKey("nodes"));
    Map<String, Object> nodes = hdfsStats.get("nodes");
    Assert.assertTrue(toInt(nodes.get("Namenodes")) > 0);
    Map<String, Object> storage = hdfsStats.get("storage");
    long totalBytes = toLong(storage.get("TotalBytes"));
    long usedBytes = toLong(storage.get("UsedBytes"));
    long remainingBytes = toLong(storage.get("RemainingBytes"));
    Assert.assertTrue(totalBytes >= 0);
    Assert.assertTrue(usedBytes >= 0);
    Assert.assertTrue(remainingBytes >= 0);
    Assert.assertTrue(totalBytes >= usedBytes + remainingBytes);
    Assert.assertTrue(toInt(storage.get("UnderReplicatedBlocks")) >= 0);
    Assert.assertTrue(toInt(storage.get("MissingBlocks")) >= 0);
    Assert.assertTrue(toInt(storage.get("CorruptBlocks")) >= 0);
  }

  @Test
  public void testYarnStats() throws Exception {
    Map<String, String> yarnInfo = getInfoStats("yarn");
    Assert.assertTrue(yarnInfo.containsKey("Version") && !yarnInfo.get("Version").isEmpty());
    Assert.assertTrue(yarnInfo.containsKey("WebURL") && !yarnInfo.get("WebURL").isEmpty());
    Assert.assertTrue(yarnInfo.containsKey("LogsURL") && !yarnInfo.get("LogsURL").isEmpty());
    Map<String, Map<String, Object>> yarnStats = getOtherServiceStats("yarn");
    Map<String, Object> resources = yarnStats.get("resources");
    int totalVCores = toInt(resources.get("TotalVCores"));
    int usedVCores = toInt(resources.get("UsedVCores"));
    int freeVCores = toInt(resources.get("FreeVCores"));
    Assert.assertEquals(totalVCores, usedVCores + freeVCores);
    long totalMemory = toLong(resources.get("TotalMemory"));
    long usedMemory = toLong(resources.get("UsedMemory"));
    long freeMemory = toLong(resources.get("FreeMemory"));
    Assert.assertEquals(totalMemory, usedMemory + freeMemory);
    Map<String, Object> nodes = yarnStats.get("nodes");
    int totalNodes = toInt(nodes.get("TotalNodes"));
    int newNodes = toInt(nodes.get("NewNodes"));
    int unusableNodes = toInt(nodes.get("UnusableNodes"));
    int healthyNodes = toInt(nodes.get("HealthyNodes"));
    Assert.assertEquals(totalNodes, newNodes + unusableNodes + healthyNodes);
    int totalContainers = toInt(nodes.get("TotalContainers"));
    int newContainers = toInt(nodes.get("NewContainers"));
    int unusableContainers = toInt(nodes.get("UnusableContainers"));
    int healthyContainers = toInt(nodes.get("HealthyContainers"));
    Assert.assertEquals(totalContainers, newContainers + unusableContainers + healthyContainers);
    Map<String, Object> apps = yarnStats.get("apps");
    int totalApps = toInt(apps.get("Total"));
    int newApps = toInt(apps.get("New"));
    int acceptedApps = toInt(apps.get("Accepted"));
    int submittedApps = toInt(apps.get("Submitted"));
    int runningApps = toInt(apps.get("Running"));
    int finishedApps = toInt(apps.get("Finished"));
    int failedApps = toInt(apps.get("Failed"));
    int killedApps = toInt(apps.get("Killed"));
    Assert.assertEquals(
      totalApps, newApps + acceptedApps + submittedApps + runningApps + finishedApps + failedApps + killedApps
    );
    Map<String, Object> queues = yarnStats.get("queues");
    int totalQueues = toInt(queues.get("Total"));
    int runningQueues = toInt(queues.get("Running"));
    int stoppedQueues = toInt(queues.get("Stopped"));
    Assert.assertEquals(totalQueues, runningQueues + stoppedQueues);
  }

  @Test
  public void testHBaseStats() throws Exception {
    Map<String, String> hbaseInfo = getInfoStats("hbase");
    Assert.assertTrue(hbaseInfo.containsKey("Version") && !hbaseInfo.get("Version").isEmpty());
    Assert.assertTrue(hbaseInfo.containsKey("WebURL") && !hbaseInfo.get("WebURL").isEmpty());
    Assert.assertTrue(hbaseInfo.containsKey("LogsURL") && !hbaseInfo.get("LogsURL").isEmpty());
    Map<String, Map<String, Object>> hbaseStats = getOtherServiceStats("hbase");
    Map<String, Object> entities = hbaseStats.get("entities");
    // HBase has default and system namespaces at the bare minimum.
    Assert.assertTrue(toInt(entities.get("Namespaces")) >= 2);
    Assert.assertTrue(toInt(entities.get("Tables")) > 0);
    Assert.assertTrue(toInt(entities.get("Snapshots")) >= 0);
    Map<String, Object> nodes = hbaseStats.get("nodes");
    Assert.assertTrue(toInt(nodes.get("Masters")) >= 1);
    int regionServers = toInt(nodes.get("RegionServers"));
    Assert.assertTrue(regionServers >= 1);
    Assert.assertTrue(toInt(nodes.get("DeadRegionServers")) >= 0);
    Map<String, Object> load = hbaseStats.get("load");
    int totalRegions = toInt(load.get("TotalRegions"));
    Assert.assertTrue(totalRegions > 0);
    Assert.assertEquals(totalRegions / regionServers, toDouble(load.get("AverageRegionsPerServer")), 0.1);
    Assert.assertTrue(toInt(load.get("RegionsInTransition")) >= 0);
  }
  
  private int toInt(Object object) {
    return ((Number) object).intValue();
  }
  
  private long toLong(Object object) {
    return ((Number) object).longValue();
  }

  private double toDouble(Object object) {
    return ((Number) object).doubleValue();
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
