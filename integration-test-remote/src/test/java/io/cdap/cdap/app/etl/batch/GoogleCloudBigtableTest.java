/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.DataprocETLTestBase;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests reading from Google Bigtable and writing to Google Bigtable within a Dataproc cluster.
 */
public class GoogleCloudBigtableTest extends DataprocETLTestBase {
  private static final String BIG_TABLE_PLUGIN_NAME = "Bigtable";
  private static final String SOURCE_TABLE_NAME = "test_source_table";
  private static final String SOURCE_TABLE_NOT_EXISTING_NAME = SOURCE_TABLE_NAME + "_not_existing";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";
  private static final String SINK_TABLE_EXISTING_NAME = SINK_TABLE_NAME_TEMPLATE + "existing";
  private static final String SINK_TABLE_NEW_NAME = SINK_TABLE_NAME_TEMPLATE + "new";

  private static String instanceId;

  @BeforeClass
  public static void testGoogleBigtableSetup() throws IOException {
    instanceId = Preconditions.checkNotNull(System.getProperty("google.application.bigtable.instance"),
                                            "The instance for google bigtable is not set.");

    try (Connection connection = connect(getProjectId(), instanceId, getServiceAccountCredentials())) {
      dropTables(connection);
      createTables(connection);
      populateData(connection);
    }
  }

  private static void createTables(Connection connection) throws IOException {
    List<String> families = ImmutableList.of("cf1", "cf2");
    createTable(connection, SOURCE_TABLE_NAME, families);
    createTable(connection, SINK_TABLE_EXISTING_NAME, families);
    dropTableIfExists(connection, SINK_TABLE_NEW_NAME);
    dropTableIfExists(connection, SOURCE_TABLE_NOT_EXISTING_NAME);
  }

  private static void populateData(Connection connection) throws IOException {
    Table sourceTable = getTable(connection, SOURCE_TABLE_NAME);
    Put put1 = new Put(Bytes.toBytes("r1"));
    put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("boolean_column"), Bytes.toBytes(true));
    put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("bytes_column"), Bytes.toBytes("bytes"));
    put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("double_column"), Bytes.toBytes(10.5D));
    put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("float_column"), Bytes.toBytes(10.5F));
    put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("int_column"), Bytes.toBytes(1));
    put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("long_column"), Bytes.toBytes(10L));
    put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("string_column"), Bytes.toBytes("string"));
    sourceTable.put(put1);
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!bigTablePluginExists(dataPipelineId, BatchSource.PLUGIN_TYPE)) {
          return false;
        }
        return bigTablePluginExists(dataPipelineId, BatchSink.PLUGIN_TYPE);
      } catch (ArtifactNotFoundException e) {
        // happens if the relevant artifact(s) were not added yet
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Test
  public void testReadDataAndStoreInExistingTable() throws Exception {
    String pluginName = BIG_TABLE_PLUGIN_NAME + "-testReadDataAndStoreInExistingTable";
    DeploymentDetails deploymentDetails = deployBigtableApplication(pluginName, SOURCE_TABLE_NAME,
                                                                    SINK_TABLE_EXISTING_NAME);
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);
    verifySinkData(SINK_TABLE_EXISTING_NAME);
  }

  @Test
  public void testReadDataAndStoreInNewTable() throws Exception {
    String pluginName = BIG_TABLE_PLUGIN_NAME + "-testReadDataAndStoreInNewTable";
    DeploymentDetails deploymentDetails = deployBigtableApplication(pluginName, SOURCE_TABLE_NAME, SINK_TABLE_NEW_NAME);
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);
    verifySinkData(SINK_TABLE_NEW_NAME);
  }

  @Test
  public void testReadDataFromNotExistingTable() throws Exception {
    String pluginName = BIG_TABLE_PLUGIN_NAME + "-testReadDataFromNotExistingTable";
    DeploymentDetails deploymentDetails = deployBigtableApplication(pluginName, SOURCE_TABLE_NOT_EXISTING_NAME,
                                                                    SINK_TABLE_EXISTING_NAME);
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.FAILED);
  }

  @Override
  protected void innerTearDown() {

  }

  @AfterClass
  public static void testClassClear() throws IOException {
    try (Connection connection = connect(getProjectId(), instanceId, getServiceAccountCredentials())) {
      dropTables(connection);
    }
  }


  private DeploymentDetails deployBigtableApplication(String pluginName, String sourceTableName, String sinkTableName)
    throws Exception {
    String sourceMappings = "cf1:boolean_column=boolean_column," +
      "cf2:bytes_column=bytes_column," +
      "cf1:double_column=double_column," +
      "cf2:float_column=float_column," +
      "cf1:int_column=int_column," +
      "cf2:long_column=long_column," +
      "cf1:string_column=string_column";

    String sinkMappings = "boolean_column=cf1:boolean_column," +
      "bytes_column=cf2:bytes_column," +
      "double_column=cf1:double_column," +
      "float_column=cf2:float_column," +
      "int_column=cf1:int_column," +
      "long_column=cf2:long_column," +
      "string_column=cf1:string_column";

    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("boolean_column", Schema.of(Schema.Type.BOOLEAN)),
                      Schema.Field.of("int_column", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("long_column", Schema.of(Schema.Type.LONG)),
                      Schema.Field.of("float_column", Schema.of(Schema.Type.FLOAT)),
                      Schema.Field.of("double_column", Schema.of(Schema.Type.DOUBLE)),
                      Schema.Field.of("bytes_column", Schema.of(Schema.Type.BYTES)),
                      Schema.Field.of("string_column", Schema.of(Schema.Type.STRING))
      );

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigtable_source")
      .put("project", getProjectId())
      .put("instance", instanceId)
      .put("table", sourceTableName)
      .put("serviceFilePath", "auto-detect")
      .put("keyAlias", "id")
      .put("columnMappings", sourceMappings)
      .put("on-error", "skip-error")
      .put("schema", schema.toString())

      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigtable_sink")
      .put("project", getProjectId())
      .put("instance", instanceId)
      .put("table", sinkTableName)
      .put("serviceFilePath", "auto-detect")
      .put("keyAlias", "id")
      .put("columnMappings", sinkMappings)

      .build();

    return deployApplication(sourceProps, sinkProps, pluginName);
  }

  private void verifySinkData(String sinkTableName) throws IOException {
    try (Connection connection = connect(getProjectId(), instanceId, getServiceAccountCredentials())) {
      Table sinkTable = getTable(connection, sinkTableName);
      Result result = sinkTable.get(new Get(Bytes.toBytes("r1")));
      Assert.assertTrue(Bytes.toBoolean(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("boolean_column"))));
      Assert.assertEquals("bytes",
                          Bytes.toString(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("bytes_column"))));
      Assert.assertEquals(10.5D,
                          Bytes.toDouble(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("double_column"))),
                          0.0000001);
      Assert.assertEquals(10.5F,
                          Bytes.toFloat(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("float_column"))),
                          0.0000001);
      Assert.assertEquals(1,
                          Bytes.toInt(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("int_column"))));
      Assert.assertEquals(10L,
                          Bytes.toLong(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("long_column"))));
      Assert.assertEquals("string",
                          Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("string_column"))));
    }
  }

  private static void dropTables(Connection connection) throws IOException {
    dropTableIfExists(connection, SOURCE_TABLE_NAME);
    dropTableIfExists(connection, SOURCE_TABLE_NOT_EXISTING_NAME);
    dropTableIfExists(connection, SINK_TABLE_EXISTING_NAME);
    dropTableIfExists(connection, SINK_TABLE_NEW_NAME);
  }

  private boolean bigTablePluginExists(ArtifactId dataPipelineId, String pluginType) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM).stream()
      .anyMatch(pluginSummary -> BIG_TABLE_PLUGIN_NAME.equals(pluginSummary.getName()));
  }

  private DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                              Map<String, String> sinkProperties,
                                              String applicationName) throws Exception {
    ArtifactSelectorConfig artifact = new ArtifactSelectorConfig("SYSTEM", "google-cloud", "[0.0.0, 100.0.0)");
    ETLStage source = new ETLStage("DatastoreSourceStage",
                                   new ETLPlugin(BIG_TABLE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 sourceProperties,
                                                 artifact));
    ETLStage sink = new ETLStage("DatastoreSinkStage", new ETLPlugin(BIG_TABLE_PLUGIN_NAME,
                                                                     BatchSink.PLUGIN_TYPE,
                                                                     sinkProperties,
                                                                     artifact));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private static Connection connect(String projectId, String instanceId, @Nullable String serviceAccountCredentials) {
    Configuration configuration = BigtableConfiguration.configure(projectId, instanceId);
    if (serviceAccountCredentials != null) {
      configuration.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY,
                        serviceAccountCredentials);
    }
    return BigtableConfiguration.connect(configuration);
  }

  private static void createTable(Connection connection, String dbTableName, List<String> families) throws IOException {
    TableName tableName = TableName.valueOf(dbTableName);
    HTableDescriptor table = new HTableDescriptor(tableName);
    families.stream().map(HColumnDescriptor::new).forEach(table::addFamily);
    if (!connection.getAdmin().tableExists(tableName)) {
      connection.getAdmin().createTable(table);
    }
  }

  private static Table getTable(Connection connection, String dbTableName) throws IOException {
    TableName tableName = TableName.valueOf(dbTableName);
    return connection.getTable(tableName);
  }

  private static void dropTableIfExists(Connection connection, String dbTableName) throws IOException {
    TableName tableName = TableName.valueOf(dbTableName);
    if (connection.getAdmin().tableExists(tableName)) {
      connection.getAdmin().disableTable(tableName);
      connection.getAdmin().deleteTable(tableName);
    }
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }
}
