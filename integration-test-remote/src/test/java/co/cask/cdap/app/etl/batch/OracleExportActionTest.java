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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Integration Test for OracleExportAction
 */
public class OracleExportActionTest extends ETLTestBase {
  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord",
                    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Schema CSV_SCHEMA =
    Schema.recordOf("csvRecord",
                    Schema.Field.of("role", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("password_required", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("authentication_type", Schema.of(Schema.Type.STRING)));

  @Test
  public void testOracleExportAction() throws Exception {
    java.util.Properties oracleProperties = new java.util.Properties();
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("OracleConfig.config");
    oracleProperties.load(inputStream);
    inputStream.close();

    ETLStage readFile =
      new ETLStage("ReadFile", new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
                                             ImmutableMap.of(
                                               Properties.File.PATH, "file:///tmp/output.csv",
                                               "referenceName", "test-file",
                                               "schema", SOURCE_SCHEMA.toString()), null));

    ETLStage parse =
      new ETLStage("ReadCSV", new ETLPlugin("CSVParser", Transform.PLUGIN_TYPE,
                                            ImmutableMap.of(
                                              "format", "DEFAULT",
                                              "field", "body",
                                              "schema", CSV_SCHEMA.toString()), null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, "outputTable",
                                                Properties.Table.PROPERTY_SCHEMA, CSV_SCHEMA.toString(),
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "role"), null));

    ETLStage action = new ETLStage("action", new ETLPlugin(
      "OracleExport",
      Action.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("oracleServerHostname", oracleProperties.get("hostname").toString())
        .put("oracleServerPort", oracleProperties.get("port").toString())
        .put("oracleServerUsername", oracleProperties.get("username").toString())
        .put("authMechanism", "Private Key")
        .put("privateKey", oracleProperties.get("private.key").toString())
        .put("passphrase", "")
        .put("oracleServerPassword", "")
        .put("dbUsername", oracleProperties.get("oracle.db.user").toString())
        .put("dbPassword", oracleProperties.get("oracle.db.password").toString())
        .put("oracleHome", oracleProperties.get("oracle.db.home").toString())
        .put("oracleSID", oracleProperties.get("oracle.sid").toString())
        .put("commandToRun", oracleProperties.get("oracle.sid").toString())
        .put("queryToExecute", "select * from dba_roles;")
        .put("tmpSQLScriptFile", oracleProperties.get("oracle.db.temp.scriptfile.absolutepath").toString())
        .put("outputPath", "file:///tmp/output.csv")
        .put("format", "CSV")
        .put("oracle", "ss")
        .build(),
      null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(action)
      .addStage(readFile)
      .addStage(parse)
      .addStage(sink)
      .addConnection(action.getName(), readFile.getName())
      .addConnection(readFile.getName(), parse.getName())
      .addConnection(parse.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    Id.Application appId = Id.Application.from(TEST_NAMESPACE, "OracleExportTest");
    ApplicationManager appManager = deployApplication(appId, request);

    WorkflowManager mrManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    mrManager.start();
    mrManager.waitForFinish(10, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getTableDataset("outputTable");
    Table outputTable = outputManager.get();
    Row row = outputTable.get(Bytes.toBytes("DBA"));
    Assert.assertEquals("NO", row.getString("password_required"));
  }
}
