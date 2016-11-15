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
package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BatchSolrSearchTest extends ETLTestBase {
  private static final String SINGLE_NODE_MODE = "SingleNode";
  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "input-record",
    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("firstname_s", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("lastname_s", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("office address", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("pincode_i", Schema.nullableOf(Schema.of(Schema.Type.INT))));

  private SolrClient client;

  @Test
  public void testSolrSingleNodeMode() throws Exception {
    // Test case requires the below used Solr server to be always running, with the mentioned collection.
    String solrDataSource = "solr-single-node-mode";

    ETLStage sourceStage = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, solrDataSource,
      Properties.Table.PROPERTY_SCHEMA, INPUT_SCHEMA.toString()), null));

    Map<String, String> sinkConfigProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "BatchSolrSink")
      .put("solrMode", SINGLE_NODE_MODE)
      .put("solrHost", "104.198.198.29:8983")
      .put("collectionName", "hydrator")
      .put("keyField", "id")
      .put("batchSize", "10000")
      .put("outputFieldMappings", "office address:address_s")
      .build();

    ETLStage sinkStage = new ETLStage("SolrSearchSink", new ETLPlugin("SolrSearch", BatchSink.PLUGIN_TYPE,
                                                                      sinkConfigProperties, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(sourceStage)
      .addStage(sinkStage)
      .addConnection(sourceStage.getName(), sinkStage.getName())
      .build();

    ingestInputData(solrDataSource);

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.toEntityId().app("solr-search-test");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    client = new HttpSolrClient("http://104.198.198.29:8983/solr/hydrator");
    QueryResponse queryResponse = client.query(new SolrQuery("*:*"));
    SolrDocumentList resultList = queryResponse.getResults();

    Assert.assertEquals(4, resultList.size());
    for (SolrDocument document : resultList) {
      if (document.get("id").equals("BL")) {
        Assert.assertEquals("Brett", document.get("firstname_s"));
        Assert.assertEquals("Lee", document.get("lastname_s"));
        Assert.assertEquals("NE lake side", document.get("address_s"));
        Assert.assertEquals(480001, document.get("pincode_i"));
      } else if (document.get("id").equals("JR")) {
        Assert.assertEquals("John", document.get("firstname_s"));
        Assert.assertEquals("Ray", document.get("lastname_s"));
        Assert.assertEquals("SE lake side", document.get("address_s"));
        Assert.assertEquals(480002, document.get("pincode_i"));
      } else if (document.get("id").equals("JW")) {
        Assert.assertEquals("Johnny", document.get("firstname_s"));
        Assert.assertEquals("Wagh", document.get("lastname_s"));
        Assert.assertEquals("EE lake side", document.get("address_s"));
        Assert.assertEquals(480003, document.get("pincode_i"));
      } else {
        Assert.assertEquals("Michael", document.get("firstname_s"));
        Assert.assertEquals("Hussey", document.get("lastname_s"));
        Assert.assertEquals("WE lake side", document.get("address_s"));
        Assert.assertEquals(480004, document.get("pincode_i"));
      }
    }
    // Clean the indexes
    client.deleteByQuery("*:*");
    client.commit();
    client.shutdown();
  }

  private void ingestInputData(String inputDatasetName) throws Exception {
    DataSetManager<Table> inputManager = getTableDataset(inputDatasetName);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, "BL", "Brett", "Lee", "NE lake side", 480001);
    putValues(inputTable, 2, "JR", "John", "Ray", "SE lake side", 480002);
    putValues(inputTable, 3, "JW", "Johnny", "Wagh", "EE lake side", 480003);
    putValues(inputTable, 4, "MH", "Michael", "Hussey", "WE lake side", 480004);
    inputManager.flush();
  }

  private void putValues(Table inputTable, int index, String id, String firstName, String lastName, String address,
                         int pinCode) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("id", id);
    put.add("firstname_s", firstName);
    put.add("lastname_s", lastName);
    put.add("office address", address);
    put.add("pincode_i", pinCode);
    inputTable.put(put);
  }
}
