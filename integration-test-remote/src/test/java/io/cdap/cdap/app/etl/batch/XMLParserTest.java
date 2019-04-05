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

package io.cdap.cdap.app.etl.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Properties;
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
    ApplicationId appId = TEST_NAMESPACE.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId, request);

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
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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
    ApplicationId appId = TEST_NAMESPACE.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId, request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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
    ApplicationId appId = TEST_NAMESPACE.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId, request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type>" +
      "</subcategory><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author>" +
      "<year>2005</year><price>30.00</price></book>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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
    ApplicationId appId = TEST_NAMESPACE.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId, request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
      "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book></bookstore>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);

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
    
    ETLStage source =
      new ETLStage("TableSource", new ETLPlugin("Table", BatchSource.PLUGIN_TYPE,
                                                ImmutableMap.of(
                                                  Properties.BatchReadableWritable.NAME, xmlParserSource,
                                                  Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()),
                                                null));

    Map<String, String> transformProperties = new ImmutableMap.Builder<String, String>()
      .put("input", "body")
      .put("encoding", "UTF-8")
      .put("xPathMappings", "category://book/@category,title://book/title,year:/bookstore/book[price>35.00]/year," +
        "price:/bookstore/book[price>35.00]/price,subcategory://book/subcategory")
      .put("fieldTypeMapping", "category:string,title:string,price:double,year:int,subcategory:string")
      .put("processOnError", "Exit on error")
      .build();

    ETLStage transform =
      new ETLStage("transform", new ETLPlugin("XMLParser", Transform.PLUGIN_TYPE, transformProperties, null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, xmlParserSink,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "category",
                                                Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()),
                                              null));

    ETLBatchConfig etlBatchConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlBatchConfig);
    ApplicationId appId = TEST_NAMESPACE.app("XMLParserTest");
    ApplicationManager appManager = deployApplication(appId, request);

    DataSetManager<Table> inputManager = getTableDataset(xmlParserSource);
    Table inputTable = inputManager.get();

    putValues(inputTable, 1, 1, "<bookstore><book category=\"cooking\"><subcategory><type>Continental</type>" +
      "</subcategory><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author>" +
      "<year>2005</year><price>30.00</price></book><book category=\"children\"><subcategory><type>Series</type>" +
      "</subcategory><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>" +
      "49.99</price></book></bookstore>");
    inputManager.flush();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.MINUTES);
    Assert.assertEquals(ProgramRunStatus.FAILED, workflowManager.getHistory().get(0).getStatus());

    DataSetManager<Table> outputManager = getTableDataset(xmlParserSink);
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("cooking"));
    Assert.assertTrue(row.isEmpty());
  }
}
