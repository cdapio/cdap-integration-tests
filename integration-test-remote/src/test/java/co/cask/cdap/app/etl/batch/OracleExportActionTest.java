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
import org.junit.Before;
import org.junit.Test;

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

  private static final String PRIVATE_KEY =
    "-----BEGIN RSA PRIVATE KEY-----\n" +
      "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\n" +
      "-----END RSA PRIVATE KEY-----";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testOracleExportAction() throws Exception {
    ETLStage readFile =
      new ETLStage("ReadFile", new ETLPlugin("File", BatchSource.PLUGIN_TYPE,
                                            ImmutableMap.of(
                                              Properties.File.PATH, "file:///tmp/output.csv",
                                              "referenceName" , "test-file",
                                              "schema", SOURCE_SCHEMA.toString()), null));

    ETLStage parse =
      new ETLStage("ReadCSV", new ETLPlugin("CSVParser", Transform.PLUGIN_TYPE,
                                            ImmutableMap.of(
                                              "format" , "DEFAULT",
                                                "field", "body",
                                                "schema", CSV_SCHEMA.toString()), null));

    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, "outputTable",
                                                Properties.Table.PROPERTY_SCHEMA, CSV_SCHEMA.toString(),
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "role"), null));

    String oracleCommand = "docker exec -i coopr-oracle-xe-11g bash -c " +
      "'/u01/app/oracle-product/11.2.0/xe/bin/sqlplus -s " +
      "system/oracle@//localhost:1521/xe @/u01/app/oracle/tempsql.sql'";

    ETLStage action = new ETLStage("action", new ETLPlugin(
      "OracleExport",
      Action.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put("oracleServerHostname", "104.154.205.167")
        .put("oracleServerPort", "22")
        .put("oracleServerUsername", "coopr")
        .put("authMechanism", "Private Key")
        .put("privateKey", PRIVATE_KEY)
        .put("passphrase", "")
        .put("oracleServerPassword", "")
        .put("dbUsername", "system")
        .put("dbPassword", "oracle")
        .put("oracleHome", "/u01/app/oracle/product/11.2.0/xe")
        .put("oracleSID", "xe")
        .put("commandToRun", oracleCommand)
        .put("queryToExecute", "select * from dba_roles;")
        .put("tmpSQLScriptFile", "/data/oracledb-xe-11g/tempsql.sql")
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
