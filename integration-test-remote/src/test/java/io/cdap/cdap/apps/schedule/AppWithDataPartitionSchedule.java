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

package io.cdap.cdap.apps.schedule;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.AppWithMultipleWorkflows;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * App with a schedule triggered by new partition in the dataset with name "rawRecords"
 * in {@link io.cdap.cdap.examples.datacleansing.DataCleansing}.
 */
public class AppWithDataPartitionSchedule extends AbstractApplication<AppWithDataPartitionSchedule.AppConfig> {
  public static final String NAME = "AppWithDataPartitionSchedule";
  public static final String TWO_ACTIONS_WORKFLOW = "TwoActionsWorkflow";
  public static final String DELAY_WORKFLOW = "DelayWorkflow";
  public static final String TIME_TRIGGER_ONLY_WORKFLOW = "TimeTriggerOnlyWorkflow";
  public static final String CONCURRENCY_SCHEDULE = "ConcurrencySchedule";
  public static final String DELAY_SCHEDULE = "DelaySchedule";
  public static final String CAN_FAIL_SCHEDULE = "CanFailSchedule";
  public static final String TIME_SCHEDULE = "TimeSchedule";
  public static final String FAIL_KEY = "fail_key";
  public static final String FAIL_VALUE = "fail_value";
  public static final Map<String, String> WORKFLOW_FAIL_PROPERTY = ImmutableMap.of(FAIL_KEY, FAIL_VALUE);
  public static final int TRIGGER_ON_NUM_PARTITIONS = 5;
  public static final long ACTION_SLEEP_SECONDS = 10;
  public static final int DELAY_MILLIS = 10000;
  public static final long MIN_SINCE_LAST_RUN = 10; // 10 minutes

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Sample application with data partition triggered schedule");
    AppConfig config = getConfig();
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
    if (config.timeTriggerOnly) {
      // Schedule TIME_TRIGGER_ONLY_WORKFLOW to wait for new partitions in rawRecords dataset
      // but always fail to complete if it's launched
      schedule(buildSchedule(CAN_FAIL_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
                 .setProperties(WORKFLOW_FAIL_PROPERTY)
                 .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS));
      // Schedule TIME_TRIGGER_ONLY_WORKFLOW to run every minute but only after MIN_SINCE_LAST_RUN minutes
      // since last workflow completion
      schedule(buildSchedule(TIME_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
                 .withDurationSinceLastRun(MIN_SINCE_LAST_RUN, TimeUnit.MINUTES)
                 .triggerByTime("* * * * ?"));
    } else {
      // Schedule TIME_TRIGGER_ONLY_WORKFLOW to wait for new partitions in rawRecords dataset
      schedule(buildSchedule(CAN_FAIL_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
                 .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS));
      // Schedule TIME_TRIGGER_ONLY_WORKFLOW to run every minute within the time window but always fail
      schedule(buildSchedule(TIME_SCHEDULE, ProgramType.WORKFLOW, TIME_TRIGGER_ONLY_WORKFLOW)
                 .withTimeWindow(config.startTime, config.endTime, TimeZone.getTimeZone(config.timeZone))
                 .abortIfNotMet()
                 .setProperties(WORKFLOW_FAIL_PROPERTY)
                 .triggerByTime("* * * * ?"));
    }
  }

  /**
   * Application Config Class to control schedule creation
   */
  public static class AppConfig extends Config {
    private final boolean timeTriggerOnly;
    private final String startTime;
    private final String endTime;
    private final String timeZone;

    public AppConfig() {
      this.timeTriggerOnly = true;
      this.startTime = null;
      this.endTime = null;
      this.timeZone = null;
    }

    public AppConfig(String startTime, String endTime, String timeZone) {
      this.timeTriggerOnly = false;
      this.startTime = startTime;
      this.endTime = endTime;
      this.timeZone = timeZone;
    }
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
        TimeUnit.SECONDS.sleep(ACTION_SLEEP_SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted.", e);
      }
    }
  }
}
