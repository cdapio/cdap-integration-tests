/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.chaosmonkey.ChaosMonkeyService;
import co.cask.chaosmonkey.Clusters;
import co.cask.chaosmonkey.common.Constants;
import co.cask.chaosmonkey.common.conf.Configuration;
import co.cask.chaosmonkey.proto.ClusterDisrupter;
import co.cask.chaosmonkey.proto.ClusterInfoCollector;
import org.junit.BeforeClass;

/**
 * Wrapper around AudiTestBase that includes ChaosMonkeyService
 */
public class DisruptionTestBase extends AudiTestBase {

  private final ClusterDisrupter clusterDisrupter;
  private static Configuration conf;

  @BeforeClass
  public static void confSetup() {
    conf = Configuration.create();

    String tenantId = System.getProperty("coopr.tenant.id", "cask-dev");
    String cooprUrlAndPort = System.getProperty("coopr.url.and.port", "http://coopr-dev.dev.continuuity.net:55054");
    String cooprClusterId = System.getProperty("coopr.cluster.id");

    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.CLUSTER_ID, cooprClusterId);
    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.TENANT_ID, tenantId);
    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.SERVER_URI, cooprUrlAndPort);

    String sshUsername = System.getProperty("ssh.username", null);
    String sshKeyPassphrase = System.getProperty("ssh.key.passphrase", null);
    String privateKey = System.getProperty("ssh.private.key", null);

    if (sshUsername != null) {
      conf.set("username", sshUsername);
    }
    if (sshKeyPassphrase != null) {
      conf.set("keyPassphrase", sshKeyPassphrase);
    }
    if (privateKey != null) {
      conf.set("privateKey", privateKey);
    }
  }

  public DisruptionTestBase() {
    super();
    try {
      ClusterInfoCollector clusterInfoCollector = Clusters.createInitializedInfoCollector(conf);
      ChaosMonkeyService chaosMonkeyService = new ChaosMonkeyService(conf, clusterInfoCollector);
      chaosMonkeyService.start();
      clusterDisrupter = chaosMonkeyService;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected ClusterDisrupter getClusterDisrupter() {
    return clusterDisrupter;
  }
}
