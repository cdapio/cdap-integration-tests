/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.test;

import io.cdap.chaosmonkey.ChaosMonkeyService;
import io.cdap.chaosmonkey.Clusters;
import io.cdap.chaosmonkey.common.Constants;
import io.cdap.chaosmonkey.common.conf.Configuration;
import io.cdap.chaosmonkey.proto.ClusterDisruptor;
import io.cdap.chaosmonkey.proto.ClusterInfoCollector;

import javax.annotation.Nullable;

/**
 * Factory that creates Chaos Monkey {@link Disruptor} objects

 */
public class DisruptorFactory {

  private static ChaosMonkeyService clusterDisruptor;
  private Configuration conf;

  public DisruptorFactory() {
    conf = Configuration.create();
  }

  public void disruptorSetup() {
    String tenantId = System.getProperty("coopr.tenant.id", "cask-dev");
    String cooprUrlAndPort = System.getProperty("coopr.url.and.port", "http://coopr-dev.dev.continuuity.net:55054");
    String cooprClusterId = System.getProperty("coopr.cluster.id");

    String sshUsername = System.getProperty("ssh.username", null);
    @Nullable
    String sshKeyPassphrase = System.getProperty("ssh.key.passphrase", null);
    String privateKey = System.getProperty("ssh.private.key", null);

    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.CLUSTER_ID, cooprClusterId);
    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.TENANT_ID, tenantId);
    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.SERVER_URI, cooprUrlAndPort);

    if (sshUsername != null) {
      this.conf.set("username", sshUsername);
    }
    if (sshKeyPassphrase != null) {
      this.conf.set("keyPassphrase", sshKeyPassphrase);
    }
    if (privateKey != null) {
      this.conf.set("privateKey", privateKey);
    }

    conf.set("hbase-master.disruptions", "io.cdap.cdap.plugins.MajorCompact," +
      Constants.Plugins.DEFAULT_DISRUPTIONS);
  }

  public void disruptorStart() throws Exception {
    disruptorSetup();
    ClusterInfoCollector clusterInfoCollector = Clusters.createInitializedInfoCollector(this.conf);
    ChaosMonkeyService chaosMonkeyService = new ChaosMonkeyService(this.conf, clusterInfoCollector);
    chaosMonkeyService.startAsync().awaitRunning();
    clusterDisruptor = chaosMonkeyService;
  }

  public void disruptorStop() {
    if (clusterDisruptor != null) {
      clusterDisruptor.stopAsync().awaitTerminated();
    }
  }

  @Nullable
  public ClusterDisruptor getClusterDisruptor() {
    return clusterDisruptor;
  }
}
