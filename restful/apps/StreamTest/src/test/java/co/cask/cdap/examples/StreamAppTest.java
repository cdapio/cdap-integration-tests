/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.examples;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
/**
 *
 */
public class StreamAppTest extends TestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
  ApplicationManager appManager = deployApplication(StreamTest.class);
  appManager.startFlow("CountFlow");

  sendData(appManager, "");
  sendData(appManager, " a b ");

  RuntimeMetrics flowMetrics = RuntimeStats.getFlowletMetrics("StreamTest", "CountFlow", "counter");
  flowMetrics.waitForProcessed(2, 3, TimeUnit.SECONDS);

  verifyGetCountProcedure(appManager);
  }

  private void sendData(ApplicationManager appManager, String str) throws IOException {
    StreamWriter stream = appManager.getStreamWriter("input");
    stream.send(Bytes.toBytes(GSON.toJson(str)));
  }

  private void verifyGetCountProcedure(ApplicationManager appManager) throws IOException {
    ProcedureManager procedureManager = appManager.startProcedure(StreamTest.GetCount.class.getSimpleName());
    ProcedureClient client = procedureManager.getClient();

    String response = client.query("get", ImmutableMap.of("token", "a"));
    Map<String, Long> counts = GSON.fromJson(response, new TypeToken<Map<String, Long>>() {
    }.getType());
    Assert.assertEquals(1L, counts.get("count").longValue());

    response = client.query("total", Collections.< String, String >emptyMap());
    counts = GSON.fromJson(response, new TypeToken<Map<String, Long>>(){ }.getType());
    Assert.assertEquals(5L, counts.get("count").longValue());
  }
}
