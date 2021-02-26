/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.proto.id.ArtifactId;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utility class for testing BigQuery plugins
 */
public class GoogleBigQueryUtils {

  public static BigQuery getBigQuery(String projectId, String credentials) throws IOException {
    try (InputStream inputStream = new ByteArrayInputStream(
      credentials.getBytes(StandardCharsets.UTF_8))) {
      return BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(GoogleCredentials.fromStream(inputStream))
        .build()
        .getService();
    }
  }

  public static void createTestTable(BigQuery bq, String datasetId, String tableId,
                                     Field[] fieldsSchema) {
    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fieldsSchema);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);

    createTestTable(bq, datasetId, tableId, tableDefinition);
  }

  public static void createTestTable(BigQuery bq, String datasetId, String tableId, TableDefinition tableDefinition) {
    TableId table = TableId.of(datasetId, tableId);
    TableInfo.newBuilder(table, tableDefinition).build();
    TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();

    bq.create(tableInfo);
  }

  public static String getUUID() {
    return UUID.randomUUID().toString().replaceAll("-", "_");
  }

  public static void insertData(BigQuery bq, Dataset dataset, String tableId,
                                Collection<JsonObject> source)
    throws IOException, InterruptedException {
    TableId table = TableId.of(dataset.getDatasetId().getDataset(), tableId);

    WriteChannelConfiguration writeChannelConfiguration =
      WriteChannelConfiguration.newBuilder(table).setFormatOptions(FormatOptions.json()).build();

    JobId jobId = JobId.newBuilder().setLocation(dataset.getLocation()).build();
    TableDataWriteChannel writer = bq.writer(jobId, writeChannelConfiguration);

    String sourceString = source.stream().map(JsonElement::toString)
      .collect(Collectors.joining("\n"));
    try (OutputStream outputStream = Channels.newOutputStream(writer);
         InputStream inputStream = new ByteArrayInputStream(
           sourceString.getBytes(Charset.forName("UTF-8")))) {
      ByteStreams.copy(inputStream, outputStream);
    }

    Job job = writer.getJob();
    job.waitFor();
  }

  public static boolean bigQueryPluginExists(ArtifactClient artifactClient,
                                             ArtifactId dataPipelineId,
                                             String pluginType, String pluginName) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM)
      .stream()
      .anyMatch(pluginSummary -> pluginName.equals(pluginSummary.getName()));
  }

  public static List<FieldValueList> getResultTableData(BigQuery bq, TableId tableId,
                                                        com.google.cloud.bigquery.Schema schema) {
    TableResult tableResult = bq.listTableData(tableId, schema);
    List<FieldValueList> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::add);
    return result;
  }
}
