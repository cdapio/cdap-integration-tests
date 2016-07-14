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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class XMLParserTest extends ETLTestBase {
  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("inputRecord", Schema.Field.of("offset", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Schema SINK_SCHEMA =
    Schema.recordOf("inputRecord", Schema.Field.of("category", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("price", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("year", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("subcategory", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  @Test
  public void testValidXMLEvent() throws Exception {
    String xmlParserSource = "xmlParserSource-valid-xml-event";
    String xmlParserSink = "xmlParserSink-valid-xml-event";

    ETLStage source = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, xmlParserSource,
      Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Ignore error and continue")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, transformProperties, null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, xmlParserSink,
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
                                                              Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()),
                                              null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"web\"><subcategory><type>Basics</type></subcategory>" +
      "<title lang=\"en\">Learning XML</title><author>Erik T. Ray</author><year>NA</year><price>39.95</price>" +
      "</book></bookstore>");
    putValues(inputTable, 2, 2, "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type>" +
      "</subcategory><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author>" +
      "<year>2005</year><price>30.00</price></book></bookstore>");
    putValues(inputTable, 3, 3, "<bookstore><book category=\"children\"><subcategory><type>Series</type>" +
      "</subcategory><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author>" +
      "<year>2005</year><price>49.99</price></book></bookstore>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertEquals("Everyday Italian", row.getString("title"));
    Assert.assertEquals(null, row.getString("price"));
    Assert.assertEquals(null, row.getString("year"));
    Assert.assertEquals("<subcategory><type>Continental</type></subcategory>", row.getString("subcategory"));

    row = outputTable.get(Bytes.toBytes("children"));
    Assert.assertEquals("Harry Potter", row.getString("title"));
    Assert.assertEquals(49.99, row.getDouble("price"), 0.0);
    Assert.assertEquals((Integer) 2005, row.getInt("year"));
    Assert.assertEquals("<subcategory><type>Series</type></subcategory>", row.getString("subcategory"));

    row = outputTable.get(Bytes.toBytes("web"));
    Assert.assertNull(row.getString("title"));
    Assert.assertNull(row.getDouble("price"));
    Assert.assertNull(row.getInt("year"));
    Assert.assertNull(row.getString("subcategory"));
  }

  private void putValues(Table inputTable, int index, int offset, String body) {
    Put put = new Put(Bytes.toBytes(index));
    put.add("offset", offset);
    put.add("body", body);
    inputTable.put(put);
  }

  @Test
  public void testWithNoXMLRecord() throws Exception {
    String xmlParserSource = "xmlParserSource-no-xmlrecord";
    String xmlParserSink = "xmlParserSink-no-xmlrecord";

    ETLStage source = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, xmlParserSource,
      Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Ignore error and continue")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, transformProperties, null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, xmlParserSink,
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
                                                              Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()),
                                              null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertTrue(row.isEmpty());
  }

  @Test
  public void testInvalidXMLRecord() throws Exception {
    String xmlParserSource = "xmlParserSource-invalid-xmlrecord";
    String xmlParserSink = "xmlParserSink-invalid-xmlrecord";

    ETLStage source = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, xmlParserSource,
      Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Ignore error and continue")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, transformProperties, null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, xmlParserSink,
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
                                                              Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()),
                                              null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type>" +
      "</subcategory><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author>" +
      "<year>2005</year><price>30.00</price></book>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertTrue(row.isEmpty());
  }

  @Test
  public void testInvalidXPathNode() throws Exception {
    String xmlParserSource = "xmlParserSource-invalid-xpathnode";
    String xmlParserSink = "xmlParserSink-invalid-xpathnode";

    ETLStage source = new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE, ImmutableMap.of(
      Properties.BatchReadableWritable.NAME, xmlParserSource,
      Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Ignore error and continue")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, transformProperties, null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(Properties.BatchReadableWritable.NAME, xmlParserSink,
                                                              Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
                                                              Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()),
                                              null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
      "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book></bookstore>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertEquals("Everyday Italian", row.getString("title"));
    Assert.assertEquals(null, row.getString("price"));
    Assert.assertEquals(null, row.getString("year"));
    Assert.assertNull(row.getString("subcategory"));
  }

  @Test
  public void testXPathArray() throws Exception {
    String xmlParserSource = "xmlParserSource-xpath-array";
    String xmlParserSink = "xmlParserSink-xpath-array";
    
    co.cask.cdap.etl.common.ETLStage source =
      new co.cask.cdap.etl.common.ETLStage("TableSource", new Plugin("Table", ImmutableMap.of(
        Properties.BatchReadableWritable.NAME, xmlParserSource,
        Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Exit on error")
      .build();

    co.cask.cdap.etl.common.ETLStage transform =
      new co.cask.cdap.etl.common.ETLStage("transform", new Plugin("XMLParser", transformProperties, null));

    co.cask.cdap.etl.common.ETLStage sink =
      new co.cask.cdap.etl.common.ETLStage("TableSink", new Plugin("Table", ImmutableMap.of(
        Properties.BatchReadableWritable.NAME, xmlParserSink,
        Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
        Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()), null));

    co.cask.cdap.etl.batch.config.ETLBatchConfig etlBatchConfig =
      new co.cask.cdap.etl.batch.config.ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));

    AppRequest<co.cask.cdap.etl.batch.config.ETLBatchConfig> request = getBatchAppRequest(etlBatchConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type>" +
      "</subcategory><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author>" +
      "<year>2005</year><price>30.00</price></book><book category=\"children\"><subcategory><type>Series</type>" +
      "</subcategory><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>" +
      "49.99</price></book></bookstore>");
    inputManager.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(10, TimeUnit.MINUTES);
    Assert.assertEquals("FAILED", mrManager.getHistory().get(0).getStatus().name());

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertTrue(row.isEmpty());
  }
}
