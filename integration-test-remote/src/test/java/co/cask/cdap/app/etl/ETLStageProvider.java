/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.hydrator.plugin.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.hydrator.plugin.batch.source.KVTableSource;
import co.cask.hydrator.plugin.batch.source.StreamBatchSource;
import co.cask.hydrator.plugin.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.transform.ProjectionTransform;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

/**
 * A class which provides different Source, Sinks and Transforms
 */
public final class ETLStageProvider {

  /**
   * Return a {@link ETLStage} for {@link StreamBatchSource}
   * For parameters details see {@link StreamBatchSource}
   */
  public ETLStage getStreamBatchSource(String streamName, String duration, @Nullable String delay,
                                       @Nullable String format, @Nullable Schema bodySchema,
                                       @Nullable String delimiter) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(Properties.Stream.NAME, streamName);
    builder.put(Properties.Stream.DURATION, duration);
    if (delay != null) {
      builder.put(Properties.Stream.DELAY, delay);
    }
    if (format != null) {
      builder.put(Properties.Stream.FORMAT, format);
    }
    if (bodySchema != null) {
      builder.put(Properties.Stream.SCHEMA, bodySchema.toString());
    }
    if (delimiter != null) {
      builder.put("format.setting.delimiter", delimiter);
    }
    return new ETLStage(streamName, new Plugin("Stream", builder.build()));
  }

  /**
   * Return an {@link ETLStage} for {@link TimePartitionedFileSetDatasetAvroSource} or
   * {@link TimePartitionedFileSetDatasetAvroSink}
   * For parameter details see {@link TimePartitionedFileSetDatasetAvroSource} or
   * {@link TimePartitionedFileSetDatasetAvroSink}
   */
  public ETLStage getTPFS(Schema schema, String fileSetName, @Nullable String basePath,
                          @Nullable String duration, @Nullable String delay) {

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(Properties.TimePartitionedFileSetDataset.SCHEMA, schema.toString());
    builder.put(Properties.TimePartitionedFileSetDataset.TPFS_NAME, fileSetName);
    if (basePath != null) {
      builder.put(Properties.TimePartitionedFileSetDataset.BASE_PATH, basePath);
    }
    if (delay != null) {
      builder.put(Properties.TimePartitionedFileSetDataset.DELAY, delay);
    }
    if (duration != null) {
      builder.put(Properties.TimePartitionedFileSetDataset.DURATION, duration);
    }
    return new ETLStage(fileSetName, new Plugin("TPFSAvro", builder.build()));
  }

  /**
   * Returns an {@link ETLStage} of empty {@link ProjectionTransform}
   */
  public ETLStage getEmptyProjectionTranform(String stageName) {
    return new ETLStage(stageName, new Plugin("Projection", ImmutableMap.<String, String>of()));
  }

  /**
   * Returns an {@link ETLStage} of for {@link KVTableSource}
   * For parameters details see {@link KVTableSource}
   */
  public ETLStage getTableSource(String tableName, @Nullable String keyField, @Nullable String valueField) {

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(Properties.BatchReadableWritable.NAME, tableName);
    if (keyField != null) {
      builder.put(Properties.KeyValueTable.KEY_FIELD, keyField);
    }
    if (valueField != null) {
      builder.put(Properties.KeyValueTable.VALUE_FIELD, valueField);
    }

    return new ETLStage(tableName, new Plugin("KVTable", builder.build()));
  }
}
