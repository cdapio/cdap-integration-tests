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

package co.cask.cdap.apps;

import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.QueryResult;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AdapterTest extends AudiTestBase {

  @Ignore // https://issues.cask.co/browse/CDAP-1183 -- resolved now?
  @Test
  public void testAdapters() throws Exception {
    getStreamClient().create("ticker");
    getStreamClient().sendEvent("ticker", "YHOO,100,50.012");
    AdapterClient adapterClient = new AdapterClient(getClientConfig());
//    adapterClient.create("testAdapter", new Gson().fromJson("{\"type\": \"stream-conversion\", \"properties\": {\"frequency\": \"1m\", \"source.name\": \"ticker\", \"sink.name\": \"ticker_converted\", \"source.format.name\": \"csv\", \"source.schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"rec\\\",\\\"fields\\\":[{\\\"name\\\":\\\"ticker\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"trades\\\",\\\"type\\\":[\\\"int\\\",\\\"null\\\"]},{\\\"name\\\":\\\"price\\\",\\\"type\\\":[\\\"double\\\",\\\"null\\\"]}]}\"}, \"source\": {\"name\":\"ticker\", \"properties\": {}}, \"sink\": {\"name\": \"ticker_converted\", \"properties\": {\"explore.table.property.avro.schema.literal\" : \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"rec\\\",\\\"fields\\\":[{\\\"name\\\":\\\"ticker\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"trades\\\",\\\"type\\\":[\\\"int\\\",\\\"null\\\"]},{\\\"name\\\":\\\"price\\\",\\\"type\\\":[\\\"double\\\",\\\"null\\\"]}]}\"}} }",
//                                                            AdapterConfig.class));
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          RESTClient restClient = new RESTClient(getClientConfig());
          HttpResponse response = restClient.execute(HttpMethod.GET, getClientConfig().resolveURL("apps/stream-conversion/workflows/StreamConversionWorkflow/runs"), getClientConfig().getAccessToken());
          List<Map<String, String>> responseObject = ObjectResponse.fromJsonBody(response, new TypeToken<List<Map<String, String>>>() {
          }).getResponseObject();
          // Wait for a run to complete (the runs can overlap)
          System.out.println(responseObject);
          for (Map<String, String> stringStringMap : responseObject) {
            if ("COMPLETED".equals(stringStringMap.get("status"))) {
              return true;
            }
          }
          return false;
        }
      }, 2, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

      QueryClient queryClient = new QueryClient(getClientConfig());
      ExploreExecutionResult queryResult = queryClient.execute("SELECT * FROM cdap_user_ticker_converted").get();
      Assert.assertTrue(queryResult.canContainResults());
      Assert.assertNotNull(queryResult.getResultSchema());
      List<QueryResult> results = Lists.newArrayList(queryResult);
      Assert.assertEquals(1, results.size());
      List<Object> columns = results.get(0).getColumns();
      Assert.assertEquals("YHOO", columns.get(0));
      Assert.assertEquals(100, columns.get(1));
      Assert.assertEquals(50.012, columns.get(2));
      //TODO: should we assert against the year, month, day, hour, minute as well?
    } finally {
      adapterClient.delete("testAdapter");
    }
  }
}
