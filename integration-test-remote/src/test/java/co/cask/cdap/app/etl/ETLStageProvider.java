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
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.hydrator.plugin.batch.source.StreamBatchSource;
import co.cask.hydrator.plugin.common.Properties;
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
    return new ETLStage(streamName, new ETLPlugin("Stream", BatchSource.PLUGIN_TYPE, builder.build(), null));
  }
}
