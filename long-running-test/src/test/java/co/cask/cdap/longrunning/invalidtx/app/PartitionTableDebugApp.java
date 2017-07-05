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

package co.cask.cdap.longrunning.invalidtx.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 */
public class PartitionTableDebugApp extends AbstractApplication {

  public static final String APP_NAME = "PartitionTableDebugApp";
  public static final String SERVICE_NAME = "PartitionScannerService";

  public static final String RAW_RECORDS = "rawRecords";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Scans the partition table of a PartitionedFileSet, looking for any inconsistencies in the index.");
    addService(SERVICE_NAME, new PartitionScannerHandler());
    addWorker(new PartitionCreateWorker());

    // Create the "rawRecords" partitioned file set (wrapped by a custom dataset) for storing the input records,
    createDataset(RAW_RECORDS, PFSWrapper.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setDescription("Store input records")
      .build());
  }
}
