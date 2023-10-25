/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.gcp;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.Tasks;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests Big Query Execute Plugin, action before and after pipeline run.
 */
public class GoogleBigQueryExecuteTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryTest.class);
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String BIG_QUERY_EXECUTE_PLUGIN_NAME = "BigQueryExecute";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";

  private static final Field[] SIMPLE_FIELDS_SCHEMA = new Field[]{
    Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
    Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
  };

  private static String bigQueryDataset;
  private static Dataset dataset;
  private static BigQuery bq;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    UUID uuid = UUID.randomUUID();
    bigQueryDataset = "bq_dataset_" + uuid.toString().replaceAll("-", "_");
    bq = GoogleBigQueryUtils.getBigQuery(getProjectId(), getServiceAccountCredentials());
    createDataset();
  }

  @AfterClass
  public static void testClassClear() {
    deleteDatasets();
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSource.PLUGIN_TYPE, BIG_QUERY_PLUGIN_NAME)) {
          return false;
        }
        return GoogleBigQueryUtils
          .bigQueryPluginExists(artifactClient, dataPipelineId, BatchSink.PLUGIN_TYPE, BIG_QUERY_PLUGIN_NAME);
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {

  }

  @Test
  public void testBigQueryExecutePlugin() throws Exception {
    String testId = GoogleBigQueryUtils.getUUID();

    String sourceTableName = String.format("%s%s", SOURCE_TABLE_NAME_TEMPLATE, testId);
    String destinationTableName = String.format("%s%s", SINK_TABLE_NAME_TEMPLATE, testId);

    String sqlBefore = String.format("INSERT INTO %s.%s VALUES('test_value', 1)", bigQueryDataset, sourceTableName);
    String sqlAfter = String.format("DROP TABLE %s.%s", bigQueryDataset, sourceTableName);

    GoogleBigQueryUtils.createTestTable(bq, bigQueryDataset, sourceTableName, SIMPLE_FIELDS_SCHEMA);
    Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${srcTable}")
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", "${project}")
      .put("dataset", "${dataset}")
      .put("table", "${dstTable}")
      .put("operation", "${operation}")
      .put("allowSchemaRelaxation", "${relax}")
      .build();

    ETLStage source = new ETLStage("BigQuerySourceStage",
                                   new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProps,
                                                 GOOGLE_CLOUD_ARTIFACT));
    ETLStage sink = new ETLStage("BigQuerySinkStage",
                                 new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProps,
                                               GOOGLE_CLOUD_ARTIFACT));
    ETLStage actionBefore = new ETLStage("BigQueryExecuteActionBefore",
                                    new ETLPlugin(BIG_QUERY_EXECUTE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                                  getActionProps(sqlBefore), GOOGLE_CLOUD_ARTIFACT));
    ETLStage actionAfter = new ETLStage("BigQueryExecuteActionAfter",
                                   new ETLPlugin(BIG_QUERY_EXECUTE_PLUGIN_NAME, Action.PLUGIN_TYPE,
                                                 getActionProps(sqlAfter), GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addStage(actionBefore)
      .addStage(actionAfter)
      .addConnection(actionBefore.getName(), source.getName())
      .addConnection(source.getName(), sink.getName())
      .addConnection(sink.getName(), actionAfter.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(BIG_QUERY_EXECUTE_PLUGIN_NAME);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("dataset", bigQueryDataset);
    args.put("srcTable", sourceTableName);
    args.put("dstTable", destinationTableName);
    args.put("operation", "INSERT");
    args.put("relax", "false");
    startWorkFlow(applicationManager, ProgramRunStatus.COMPLETED, args);

    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    int expectedCount = 1;

    checkMetric(tags, String.format("user.%s.records.out", source.getName()), expectedCount, 10);
    checkMetric(tags, String.format("user.%s.records.out", sink.getName()), expectedCount, 10);
    assertTables(sourceTableName, destinationTableName);
  }

  private Schema getSimpleTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
      );
  }

  private Map<String, String> getActionProps(String sql) {
   return new ImmutableMap.Builder<String, String>()
      .put("project", "${project}")
      .put("sql", sql)
      .put("mode", "batch")
      .put("dialect", "standard")
      .put("useCache", "false")
      .put("location", "US")
      .put("rowAsArguments", "false")
      .build();
  }

  private void assertTables(String sourceTableName, String destinationTableName) {
    TableId sourceTableId = TableId.of(bigQueryDataset, sourceTableName);
    Assert.assertNull(bq.getTable(sourceTableId));

    TableId destinationTableId = TableId.of(bigQueryDataset, destinationTableName);
    com.google.cloud.bigquery.Schema schema = bq.getTable(destinationTableId).getDefinition().getSchema();

    List<FieldValueList> resultTableData = GoogleBigQueryUtils.getResultTableData(bq, destinationTableId, schema);
    FieldValueList fieldValues = resultTableData.get(0);

    Assert.assertEquals(1, resultTableData.size());
    Assert.assertEquals("test_value", fieldValues.get("string_value").getValue());
    Assert.assertEquals("1", fieldValues.get("int_value").getValue());
  }

  private static void createDataset() {
    LOG.info("Creating dataset {}", bigQueryDataset);
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDataset).build();
    dataset = bq.create(datasetInfo);
    LOG.info("Created dataset {}", bigQueryDataset);
  }

  private static void deleteDatasets() {
    LOG.info("Deleting dataset {}", bigQueryDataset);
    boolean deleted = bq.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
    if (deleted) {
      LOG.info("Deleted dataset {}", bigQueryDataset);
    }
  }
}
