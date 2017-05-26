package co.cask.cdap.apps.schedule;

import co.cask.cdap.AppWithFrequentScheduledWorkflows;
import co.cask.cdap.api.app.AbstractApplication;

/**
 * App with a schedule triggered by new partition in the dataset with name "rawRecords"
 * in {@link co.cask.cdap.examples.datacleansing.DataCleansing}
 */
public class AppWithDataPartitionSchedule extends AbstractApplication {
  public static final String SOME_WORKFLOW = "SomeWorkflow";
  public static final String DATASET_PARTITION_SCHEDULE_1 = "DataSetPartionSchedule1";
  public static final int TRIGGER_ON_NUM_PARTITIONS = 5;


  @Override
  public void configure() {
    setName("AppWithDataPartitionSchedule");
    setDescription("Sample application with data partition triggered schedule");
    addWorkflow(new AppWithFrequentScheduledWorkflows.DummyWorkflow(SOME_WORKFLOW));
    configureWorkflowSchedule(DATASET_PARTITION_SCHEDULE_1, SOME_WORKFLOW)
      .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS);
  }
}
