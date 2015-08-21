/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps.wordcount;

import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ReadlessIncrementTest extends AudiTestBase {
  private static final String word = "word";

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws Exception {
//    ApplicationManager applicationManager = deployApplication(WordCount.class);
//    FlowManager flowManager = applicationManager.startFlow("WordCounter");
//    TimeUnit.SECONDS.sleep(30);
//    Assert.assertTrue(flowManager.isRunning());

    ClientConfig clientConfig = getClientConfig();
    clientConfig.setNamespace(Id.Namespace.from("ns1"));
    StreamClient streamClient = new StreamClient(clientConfig, getRestClient());
//    ServiceManager retrieveCounts = applicationManager.startService("RetrieveCounts");
//    waitForService(retrieveCounts);

//    RuntimeMetrics counterMetrics = flowManager.getFlowletMetrics("splitter");

    int numWordsProcessed = 0;
    Stopwatch sw = new Stopwatch().start();
    boolean doExit = false;
    while (!doExit) {
      for (int j = 0; j < 10000; j++) {
        StringWriter writer = new StringWriter();
        for (int i = 0; i < 1000; i++) {
          writer.write(word + "\n");
          numWordsProcessed++;
        }
        streamClient.sendBatch("wordStream", "text/plain",
                               ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));
      }
      int i = 2;

//      uncomment the following for 3.2.0-SNAPSHOT (104.197.126.181)
//      counterMetrics.waitForProcessed(numWordsProcessed * 4, 1, TimeUnit.MINUTES);
//      System.out.println("Processed: " + counterMetrics.getProcessed() / 4);

//      counterMetrics.waitForProcessed(numWordsProcessed, 2, TimeUnit.MINUTES);
//      System.out.println("Processed: " + counterMetrics.getProcessed());

//      if (sw.elapsedTime(TimeUnit.SECONDS) > 10) {
//        System.out.println("Total words from service: " + getWordStats(retrieveCounts).totalWords);
//        sw.reset().start();
//      }
    }
  }

  private void waitForService(ServiceManager serviceManager) throws TimeoutException, InterruptedException {
    System.out.println("Waiting for service handler...");
    Stopwatch sw = new Stopwatch().start();
    while (sw.elapsedTime(TimeUnit.SECONDS) < 60) {
      try {
        getWordStats(serviceManager);
        System.out.println("Service handler is ready after " + sw.elapsedMillis() + " ms.");
        return;
      } catch (Throwable t) {
        TimeUnit.SECONDS.sleep(1);
        System.out.println(t);
      }
    }
    throw new TimeoutException();
  }

  private WordStats getWordStats(ServiceManager serviceManager) throws IOException, UnauthorizedException {
    URL url = new URL(serviceManager.getServiceURL(), "stats");
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());
    return new Gson().fromJson(response.getResponseBodyAsString(), WordStats.class);
  }

  static class WordStats {
    double averageLength;
    int totalWords;
    int uniqueWords;
  }

}
