package co.cask.cdap.apps.adapters;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.template.etl.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.template.etl.batch.source.StreamBatchSource;
import co.cask.cdap.template.etl.batch.source.TimePartitionedFileSetDatasetAvroSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
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
    return new ETLStage("Stream", builder.build());
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
    return new ETLStage("TPFSAvro", builder.build());
  }

  /**
   * Returns an {@link ETLStage} of empty {@link ProjectionTransform}
   */
  public ETLStage getEmptyProjectionTranform() {
    return new ETLStage("Projection", ImmutableMap.<String, String>of());
  }
}
