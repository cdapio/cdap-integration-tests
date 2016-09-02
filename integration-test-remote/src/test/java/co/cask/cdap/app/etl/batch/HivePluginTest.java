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

import co.cask.cdap.api.Resources;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tests Hive source and sinks. Uses CDAP explore QueryClient to interact with hive tables directly.
 * TODO: This test uses hive plugins which are built against a specific version of hive (currently, Hive1.1-CDH5.5)
 * on a lot of distro this test will fail due to class loading issue which can only be resolved by building the hive
 * plugins against those version. So, we will need to see which all distro it fails on for the above mentioned class
 * loading issue and mark this test incompatible for those.
 */
public class HivePluginTest extends ETLTestBase {

  private static final String THRIFT = "thrift://";
  private static final int HIVE_METASTORE_PORT = 9083;
  private static final String SOURCE_TABLE = "sourcetable";
  private static final String SINK_TABLE = "sinktable";

  private QueryClient client;


  @Before
  public void createTables() throws Exception {
    if (client == null) {
      client = new QueryClient(getClientConfig());
    }
    // create source table
    ExploreExecutionResult results = client.execute(
      TEST_NAMESPACE, prepareCreateTableStatement(SOURCE_TABLE, "name string, id int, location string")).get();
    Preconditions.checkArgument(QueryStatus.OpStatus.FINISHED == results.getStatus().getStatus());

    // create sink table
    results = client.execute(
      TEST_NAMESPACE, prepareCreateTableStatement(SINK_TABLE, "name string, id int")).get();
    Preconditions.checkArgument(QueryStatus.OpStatus.FINISHED == results.getStatus().getStatus());
  }

  @After
  public void deleteTables() throws Exception {
    dropTable(client, SOURCE_TABLE);
    dropTable(client, SINK_TABLE);
  }

  @Test
  public void testKVTableWithProjection() throws Exception {
    // check that the source and sink table are empty
    verifyTableIsEmpty(SOURCE_TABLE);
    verifyTableIsEmpty(SINK_TABLE);

    // insert some dummy data in the source table
    ExploreExecutionResult results = client.execute(
      TEST_NAMESPACE, prepareInsertStatement(SOURCE_TABLE, "('name1', 1, 'miami'),('name2', 2, 'boston')")).get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    // Hive plugins needs the hive metastore to be specified in the plugin parameters. Although, the hive metastore
    // can be running on some other host than the cdap router host, the clusters provisioned for integration tests
    // always run hive and cdap router on the same host so its safe to use this host to construct the hive metastore
    // URI.
    String hiveMetastoreURI = Joiner.on(":").join(THRIFT + getInstanceURI().split(":")[0], HIVE_METASTORE_PORT);


    // create a pipeline with hive source and sink
    ETLBatchConfig etlBatchConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", new ETLPlugin("Hive", BatchSource.PLUGIN_TYPE,
                                                     ImmutableMap.of("referenceName", "hiveSource",
                                                                     "metastoreURI", hiveMetastoreURI,
                                                                     "tableName", SOURCE_TABLE),
                                                     null)))
      .addStage(new ETLStage("transform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                        ImmutableMap.of("drop", "location"), null)))
      .addStage(new ETLStage("sink", new ETLPlugin("Hive", BatchSink.PLUGIN_TYPE,
                                                   ImmutableMap.of("referenceName", "hiveSink",
                                                                   "metastoreURI", hiveMetastoreURI,
                                                                   "tableName", SINK_TABLE),
                                                   null)))
      .addConnection("source", "transform")
      .addConnection("transform", "sink")
      .setDriverResources(new Resources(1024))
      .setResources(new Resources(1024))
      .build();

    // deploy the pipeline
    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlBatchConfig);
    ApplicationId appId = new ApplicationId(TEST_NAMESPACE.getId(), "HiveTest");
    ApplicationManager appManager = deployApplication(appId.toId(), request);

    // run the workflow
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(10, TimeUnit.MINUTES);

    // verify data in sink table
    results = client.execute(TEST_NAMESPACE, prepareSelectStatement(SINK_TABLE)).get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());

    // verify that the sink has the expected schema schema
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc(Joiner.on(".").join(SINK_TABLE, "name"), "STRING", 1, null),
      new ColumnDesc(Joiner.on(".").join(SINK_TABLE, "id"), "INT", 2, null)
    );
    Assert.assertEquals(expectedSchema, results.getResultSchema());

    // verify the first record
    List<Object> columns = results.next().getColumns();
    Assert.assertEquals("name1", columns.get(0));
    Assert.assertEquals(1, (int) columns.get(1));

    // verify the second record
    columns = results.next().getColumns();
    Assert.assertEquals("name2", columns.get(0));
    Assert.assertEquals(2, (int) columns.get(1));

    // verify that there is no other records
    Assert.assertFalse("Found more record in the sink table than expected.", results.hasNext());
  }

  private void verifyTableIsEmpty(String tableName) throws InterruptedException, ExecutionException {
    ExploreExecutionResult results = client.execute(TEST_NAMESPACE, prepareSelectStatement(tableName)).get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertFalse(String.format("The table %s is not empty.", tableName), results.hasNext());
  }

  private void dropTable(QueryClient client, String tableName) throws InterruptedException, ExecutionException {
    ExploreExecutionResult results = client.execute(TEST_NAMESPACE,
                                                    String.format("DROP TABLE %s PURGE", tableName)).get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
  }

  private String prepareInsertStatement(String tableName, String values) {
    return String.format("INSERT INTO TABLE %s VALUES %s", tableName, values);
  }

  private String prepareSelectStatement(String tableName) {
    return String.format("SELECT * FROM %s", tableName);
  }

  private String prepareCreateTableStatement(String tableName, String schema) {
    return String.format("CREATE TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ':' STORED AS TEXTFILE",
                         tableName, schema);
  }
}
