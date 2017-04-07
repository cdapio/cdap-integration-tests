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
import co.cask.chaosmonkey.proto.ClusterInfoCollector;

/**
 * Wrapper around AudiTestBase that includes ChaosMonkeyService
 */
public class DisruptionTestBase extends AudiTestBase {

  private final ChaosMonkeyService chaosMonkeyService;

  public DisruptionTestBase() {
    super();

    Configuration conf = Configuration.create();
    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.CLUSTER_ID,
             System.getProperty("clusterId"));
    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.TENANT_ID, "cask-dev");
    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.SERVER_URI,
             "http://coopr-dev.dev.continuuity.net:55054");

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

    try {
      ClusterInfoCollector clusterInfoCollector = Clusters.createInitializedInfoCollector(conf);
      chaosMonkeyService = new ChaosMonkeyService(conf, clusterInfoCollector);
      chaosMonkeyService.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected ChaosMonkeyService getChaosMonkeyService() {
    return chaosMonkeyService;
  }
}
