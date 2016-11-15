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
package co.cask.cdap.app.etl.realtime;

import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RealtimeSolrSearchTest extends ETLTestBase {
  private static final String SINGLE_NODE_MODE = "SingleNode";

  @Test
  public void testSolrSingleNodeMode() throws Exception {
    // Test case requires the below used Solr server to be always running, with the mentioned collection.
    final HttpSolrClient client = new HttpSolrClient("http://104.198.198.29:8983/solr/hydrator");
    Map<String, String> sinkConfigProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SINGLE_NODE_MODE)
      .put("solrHost", "104.198.198.29:8983")
      .put("collectionName", "hydrator")
      .put("keyField", "id")
      .put("outputFieldMappings", "name:fname_s")
      .build();

    Plugin sourceConfig = new Plugin("DataGenerator",
                                     ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                                     Constants.Reference.REFERENCE_NAME, "DataGenerator"));

    Plugin sinkConfig = new Plugin("SolrSearch", sinkConfigProperties);
    Plugin transformConfig = new Plugin("Projection", ImmutableMap.of("name", "classifiedTexts",
                                                                      "drop", "score,graduated,binary,time"), null);

    ETLStage sourceStage = new ETLStage("DataGeneratorSource", sourceConfig);
    ETLStage sinkStage = new ETLStage("SolrSearchSink", sinkConfig);
    ETLStage transformStage = new ETLStage("ProjectionStage", transformConfig);
    List<ETLStage> tramsformStages = new ArrayList<ETLStage>();
    tramsformStages.add(transformStage);

    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(sourceStage, sinkStage, tramsformStages);

    AppRequest<ETLRealtimeConfig> request = getRealtimeAppRequest(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.toEntityId().app("solr-search-test");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
        return queryResponse.getResults().size() > 0;
      }
    }, 60, TimeUnit.SECONDS, 60, TimeUnit.MILLISECONDS);
    workerManager.stop();

    QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(1, resultList.size());
    SolrDocument document = resultList.get(0);
    if (document.get("id").equals("1"))
      Assert.assertEquals("Bob", document.get("fname_s"));

    // Clean the indexes
    client.deleteByQuery("*:*");
    client.commit();
    client.shutdown();
  }
}
