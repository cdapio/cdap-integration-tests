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
import co.cask.chaosmonkey.proto.ClusterDisruptor;
import co.cask.chaosmonkey.proto.ClusterInfoCollector;
import com.esotericsoftware.minlog.Log;

import javax.annotation.Nullable;

/**
 * Wrapper around AudiTestBase that includes ChaosMonkeyService
 */
public class Disruptor {

  private static ChaosMonkeyService clusterDisruptor;
  private Configuration conf;

  public Disruptor() {
    conf = Configuration.create();
  }

  public void disruptorSetup() {
    String tenantId = System.getProperty("coopr.tenant.id", "cask-dev");
    String cooprUrlAndPort = System.getProperty("coopr.url.and.port", "http://coopr-dev.dev.continuuity.net:55054");
    String cooprClusterId = System.getProperty("coopr.cluster.id");

    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.CLUSTER_ID, cooprClusterId);
    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.TENANT_ID, tenantId);
    this.conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.SERVER_URI, cooprUrlAndPort);

    String sshUsername = System.getProperty("ssh.username", null);
    @Nullable
    String sshKeyPassphrase = System.getProperty("ssh.key.passphrase", null);
    String privateKey = System.getProperty("ssh.private.key", null);

    if (sshUsername != null) {
      this.conf.set("username", sshUsername);
    }
    if (sshKeyPassphrase != null) {
      this.conf.set("keyPassphrase", sshKeyPassphrase);
    }
    if (privateKey != null) {
      this.conf.set("privateKey", privateKey);
    }

    conf.set("hbase-master.disruptions", "co.cask.cdap.plugins.MajorCompact," +
      Constants.Plugins.DEFAULT_DISRUPTIONS);
  }

  public void disruptorStart() {
    try {
      ClusterInfoCollector clusterInfoCollector = Clusters.createInitializedInfoCollector(this.conf);
      ChaosMonkeyService chaosMonkeyService = new ChaosMonkeyService(this.conf, clusterInfoCollector);
      chaosMonkeyService.startAndWait();
      clusterDisruptor = chaosMonkeyService;
    } catch (Exception e) {
      Log.error("Failed to setup Cluster Disruptor with conf: {}", conf.toString(), e);
      throw new IllegalStateException(e);
    }
  }

  public void disruptorStop() { clusterDisruptor.stop(); }

  @Nullable
  public ClusterDisruptor getClusterDisruptor() {
    return clusterDisruptor;
  }

//  public static void confSetup() {
//    conf = Configuration.create();
//
//    String tenantId = System.getProperty("coopr.tenant.id", "cask-dev");
//    String cooprUrlAndPort = System.getProperty("coopr.url.and.port", "http://coopr-dev.dev.continuity.net:55054");
//    String cooprClusterId = System.getProperty("coopr.cluster.id");
//
//    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.CLUSTER_ID, cooprClusterId);
//    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.TENANT_ID, tenantId);
//    conf.set(Constants.Plugins.CLUSTER_INFO_COLLECTOR_CONF_PREFIX + Constants.Coopr.SERVER_URI, cooprUrlAndPort);
//
//    String sshUsername = System.getProperty("ssh.username", null);
//    String sshKeyPassphrase = System.getProperty("ssh.key.passphrase", null);
//    String privateKey = System.getProperty("ssh.private.key", null);
//
//    if (sshUsername != null) {
//      conf.set("username", sshUsername);
//    }
//    if (sshKeyPassphrase != null) {
//      conf.set("keyPassphrase", sshKeyPassphrase);
//    }
//    if (privateKey != null) {
//      conf.set("privateKey", privateKey);
//    }
//
//    conf.set("hbase-master.disruptions", "co.cask.cdap.plugins.MajorCompact," +
//      Constants.Plugins.DEFAULT_DISRUPTIONS);
//
//    try {
//      ClusterInfoCollector clusterInfoCollector = Clusters.createInitializedInfoCollector(conf);
//      ChaosMonkeyService chaosMonkeyService = new ChaosMonkeyService(conf, clusterInfoCollector);
//      chaosMonkeyService.start();
//      clusterDisruptor = chaosMonkeyService;
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @AfterClass
//  public static void chaosMonkeyTearDown() {
//    clusterDisruptor.stop();
//  }
//
//  protected ClusterDisruptor getClusterDisruptor() {
//    return clusterDisruptor;
//  }
}
