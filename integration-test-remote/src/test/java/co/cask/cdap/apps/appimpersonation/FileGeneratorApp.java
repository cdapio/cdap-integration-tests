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


package co.cask.cdap.apps.appimpersonation;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.worker.AbstractWorker;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;

import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple FileGeneratorApp example.
 */
public class FileGeneratorApp extends AbstractApplication {

  private static final String RAW = "X-raw";

  @Override
  public void configure() {
    setName("FileGenerator");
    createDataset(RAW, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      // enable explore
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreSchema("record STRING")
      .build());
    addWorker(new FileGeneratorWorker());
  }

  /**
   *
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
              PartitionedFileSet XrawFileSet = datasetContext.getDataset(FileGeneratorApp.RAW);
              PartitionKey partitionKey = PartitionKey.builder().addLongField("time",
                                                                              System.currentTimeMillis()).build();
              PartitionOutput outputPartition = XrawFileSet.getPartitionOutput(partitionKey);

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
          // no-op
        }
      }
    }

    @Override
    public void stop() {
      stopped = true;
    }
  }
}
