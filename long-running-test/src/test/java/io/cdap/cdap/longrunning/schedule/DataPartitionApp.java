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

package io.cdap.cdap.longrunning.schedule;

import io.cdap.cdap.AppWithFrequentScheduledWorkflows;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * An app with time partitioned file sets and schedule triggered by new data partition in a certain dataset.
 */
public class DataPartitionApp extends AbstractApplication {
  public static final String NAME = "DataPartitionApp";
  public static final String TRIGGER_DATASET_PREFIX = "triggerDataset";
  public static final String NO_TRIGGER_DATASET = "noTriggerDataset";
  public static final String WORKFLOW_PREFIX = "dummyWorkflow";
  public static final String DATASET_PARTITION_SCHEDULE_PREFIX = "datasetPartitionSchedule";
  public static final int TRIGGER_ON_NUM_PARTITIONS = 5;
  public static final int NUM_SCHEDULES = 20;
  public static final int NUM_TRIGGER_DATASET = 5;
  public static final int NUM_SCHEDULES_PER_DATASET = NUM_SCHEDULES / NUM_TRIGGER_DATASET;


  @Override
  public void configure() {
    setName(NAME);
    for (int i = 0; i < NUM_SCHEDULES; i++) {
      String workflowName = WORKFLOW_PREFIX + i;
      String datasetName = TRIGGER_DATASET_PREFIX + (i / NUM_SCHEDULES_PER_DATASET);
      addWorkflow(new AppWithFrequentScheduledWorkflows.DummyWorkflow(workflowName));
      schedule(buildSchedule(DATASET_PARTITION_SCHEDULE_PREFIX + i, ProgramType.WORKFLOW, workflowName)
                 .triggerOnPartitions(datasetName, TRIGGER_ON_NUM_PARTITIONS));
    }

    addService(new AddDataPartitionService());

    for (int i = 0; i < NUM_TRIGGER_DATASET; i++) {
      // Create the "triggerDataset" partitioned file set for sending new partition notifications to trigger schedules
      createDataset(TRIGGER_DATASET_PREFIX + i, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
        // Properties for partitioning
        .setPartitioning(Partitioning.builder().addLongField("time").build())
        // Properties for file set
        .setInputFormat(TextInputFormat.class)
        .setDescription("Store input records")
        .build());
    }

    // Create the "noTriggerDataset" partitioned file set for sending new partition notifications
    // which cannot trigger schedules,
    createDataset(NO_TRIGGER_DATASET, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setDescription("Store input records")
      .build());
  }
}
