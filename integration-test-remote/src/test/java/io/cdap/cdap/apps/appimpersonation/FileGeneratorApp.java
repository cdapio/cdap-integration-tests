/*
 * Copyright © 2017 Cask Data, Inc.
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


package io.cdap.cdap.apps.appimpersonation;

import com.google.gson.Gson;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionOutput;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This {@code Application} generates files and stores them into a {@code PartitionedFileSet}.
 */
public class FileGeneratorApp extends AbstractApplication<FileGeneratorApp.FileGeneratorAppConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(FileGeneratorApp.class);

  public static final String RAW = "X-raw";

  public static class FileGeneratorAppConfig extends Config {
    public String resultPerms = null;
    public String resultGroup = null;
    public String resultGrants = null;
    public String resultDatabase = null;
  }

  @Override
  public void configure() {
    FileGeneratorAppConfig config = getConfig();

    PartitionedFileSetProperties.Builder builder = PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build());

    if (config.resultGrants != null) {
      Map<String, String> grants = new Gson().fromJson(config.resultGrants,
                                                       new TypeToken<Map<String, String>>() {
                                                       }.getType());
      builder.setTablePermissions(grants);
    }

    if (config.resultGroup != null) {
      builder.setFileGroup(config.resultGroup);
    }
    if (config.resultPerms != null) {
      builder.setFilePermissions(config.resultPerms);
    }

    createDataset(RAW, PartitionedFileSet.class, builder
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .build());
    addWorker(new FileGeneratorWorker());
  }

  /**
   * A {@code Worker} which generates files and stores them in a {@code PartitionedFileSet}.
   */
  public class FileGeneratorWorker extends AbstractWorker {
    private volatile boolean stopped;

    @Override
    public void run() {
      while (!stopped) {
        try {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext datasetContext) throws Exception {
              PartitionedFileSet xRawFileSet = datasetContext.getDataset(FileGeneratorApp.RAW);
              PartitionKey partitionKey = PartitionKey.builder().addLongField("time",
                                                                              System.currentTimeMillis()).build();
              PartitionOutput outputPartition = xRawFileSet.getPartitionOutput(partitionKey);

              Location partitionDir = outputPartition.getLocation();
              partitionDir.mkdirs();
              Location outputLocation = partitionDir.append("file");
              outputLocation.createNew();
              try (OutputStream outputStream = outputLocation.getOutputStream()) {
                outputStream.write(Bytes.toBytes("TestApplicationFile"));
              }
              outputPartition.addPartition();
            }
          });
          TimeUnit.SECONDS.sleep(5);
        } catch (Exception e) {
          LOG.error("An exception was thrown", e);
        }
      }
    }

    @Override
    public void stop() {
      stopped = true;
    }
  }
}
