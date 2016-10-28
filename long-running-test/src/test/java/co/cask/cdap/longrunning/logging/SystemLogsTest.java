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

package co.cask.cdap.longrunning.logging;

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.junit.Assert;

import java.net.URL;
import java.util.List;

/**
 * Tests that system logs are accessible over time.
 */
public class SystemLogsTest extends LongRunningTestBase<SystemLogTestState> {


  @Override
  public void deploy() throws Exception {
    // no-op
  }

  @Override
  public void start() throws Exception {
    // no-op
  }

  @Override
  public void stop() throws Exception {
    // no-op
  }

  @Override
  public SystemLogTestState getInitialState() {
    return new SystemLogTestState(0L, 0L);
  }

  @Override
  public void awaitOperations(SystemLogTestState state) throws Exception {
    // no-op
  }

  @Override
  public void verifyRuns(SystemLogTestState state) throws Exception {
    if (state.getT1() != 0 || state.getT2() != 0) {
      URL url = getClientConfig().resolveURLV3(String.format("system/services/appfabric/logs?start=%s&stop=%s",
                                                             state.getT1(), state.getT2()));
      HttpResponse response = getRestClient().execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
      List<String> logMessages = Lists.newArrayList(Splitter.on("\n").omitEmptyStrings()
                                                      .split(response.getResponseBodyAsString()));
      // verify that we see some log lines in system logs. This basically tests that the system logs are accessible
      // across cdap upgrades. See CDAP-7482
      Assert.assertFalse(logMessages.isEmpty());
    }
  }

  @Override
  public SystemLogTestState runOperations(SystemLogTestState state) throws Exception {
    // create a namespace to ensure some system log gets generated appfabric
    NamespaceClient namespaceClient = getNamespaceClient();
    NamespaceMeta logTestNSMeta = new NamespaceMeta.Builder().setName("logTest").build();
    namespaceClient.create(logTestNSMeta);
    namespaceClient.delete(logTestNSMeta.getNamespaceId().toId());
    return new SystemLogTestState(state.getT2(), System.currentTimeMillis() / 1000);
  }
}
