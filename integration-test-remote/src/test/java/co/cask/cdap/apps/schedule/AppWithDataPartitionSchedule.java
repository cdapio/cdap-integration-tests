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

package co.cask.cdap.apps.schedule;

import co.cask.cdap.AppWithMultipleWorkflows;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * App with a schedule triggered by new partition in the dataset with name "rawRecords"
 * in {@link co.cask.cdap.examples.datacleansing.DataCleansing}.
 */
public class AppWithDataPartitionSchedule extends AbstractApplication {
  public static final String NAME = "AppWithDataPartitionSchedule";
  public static final String TWO_ACTIONS_WORKFLOW = "TwoActionsWorkflow";
  public static final String DELAY_WORKFLOW = "DelayWorkflow";
  public static final String TIME_TRIGGER_ONLY_WORKFLOW = "TimeTriggerOnlyWorkflow";
  public static final String CONCURRENCY_SCHEDULE = "ConCurrencySchedule";
  public static final String DELAY_SCHEDULE = "DelaySchedule";
  public static final String ALWAYS_FAIL_SCHEDULE = "AlwaysFailSchedule";
  public static final String TEN_SECS_SCHEDULE = "TenSecSchedule";
  public static final int TRIGGER_ON_NUM_PARTITIONS = 5;
  public static final long WORKFLOW_RUNNING_SECONDS = 10;
  public static final int DELAY_MILLIS = 5000;
  public static final long MIN_SINCE_LAST_RUN = 10; // 10 minutes

  private static final String FAIL_KEY = "fail_key";
  private static final String FAIL_VALUE = "fail_value";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Sample application with data partition triggered schedule");
    addWorkflow(new TwoActionsWorkflow());
    addWorkflow(new CanFailWorkflow(DELAY_WORKFLOW));
    addWorkflow(new CanFailWorkflow(TIME_TRIGGER_ONLY_WORKFLOW));
    // Schedule TWO_ACTIONS_WORKFLOW to wait for new partitions in rawRecords dataset
    // with concurrency constraint 1. If concurrency constraint is not met, wait until it's met
    schedule(buildSchedule(CONCURRENCY_SCHEDULE, ProgramType.WORKFLOW, TWO_ACTIONS_WORKFLOW)
               .withConcurrency(1).waitUntilMet()
               .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS));
    // Schedule DELAY_WORKFLOW to wait for new partitions in rawRecords dataset
    // with DELAY_MILLIS delay since schedule is triggered
    schedule(buildSchedule(DELAY_SCHEDULE, ProgramType.WORKFLOW, DELAY_WORKFLOW)
               .withDelay(DELAY_MILLIS, TimeUnit.MILLISECONDS)
               .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS));
    // Schedule TIME_TRIGGER_ONLY_WORKFLOW to wait for new partitions in rawRecords dataset
    // but always fail to complete if it's launched
    schedule(buildSchedule(ALWAYS_FAIL_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
               .setProperties(ImmutableMap.of(FAIL_KEY, FAIL_VALUE))
               .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS));
    // Schedule TIME_TRIGGER_ONLY_WORKFLOW to run every 10 seconds but only after MIN_SINCE_LAST_RUN minutes
    // since last workflow completion
    schedule(buildSchedule(TEN_SECS_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
               .withDurationSinceLastRun(MIN_SINCE_LAST_RUN, TimeUnit.MINUTES)
               .triggerByTime("* * * * *"));
  }

  /**
   * A workflow that fails if runtime args contain certain key value pair.
   */
  public static class CanFailWorkflow extends AbstractWorkflow {
    private final String name;

    public CanFailWorkflow(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
      setDescription("Workflow only succeed when triggered by time");
      addAction(new CanFailAction());
    }
  }

  /**
   * An action that fails if runtime args contain certain key value pair.
   */
  public static class CanFailAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(CanFailAction.class);

    @Override
    public void run() {
      LOG.info("Running CanFailAction");
      if (FAIL_VALUE.equals(getContext().getRuntimeArguments().get(FAIL_KEY))) {
        throw new RuntimeException("CanFailAction fails");
      }
    }
  }

  /**
   * A Workflow with two actions.
   */
  public static class TwoActionsWorkflow extends AbstractWorkflow {
    @Override
    public void configure() {
      setName(TWO_ACTIONS_WORKFLOW);
      setDescription("Workflow with two actions");
      addAction(new SleepDummyAction());
      addAction(new AppWithMultipleWorkflows.SomeDummyAction());
    }
  }

  /**
   * A Dummy Action sleeps for some time.
   */
  public static class SleepDummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(SleepDummyAction.class);

    @Override
    public void run() {
      LOG.info("Running sleep dummy action");
      try {
        TimeUnit.SECONDS.sleep(WORKFLOW_RUNNING_SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted.", e);
      }
    }
  }
}
