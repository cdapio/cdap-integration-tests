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
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.Trigger;

/**
 * App with an AND trigger composed of a time trigger and a program status trigger.
 */
public class AppWithProgramStatusSchedule extends AbstractApplication {
  public static final String NAME = "AppWithProgramStatusSchedule";
  public static final String COMPOSITE_SCHEDULE = "CompositeSchedule";
  public static final String TRIGGERING_WORKFLOW = "SampleWorkflow";
  public static final String TRIGGERED_WORKFLOW = "AnotherWorkflow";

  @Override
  public void configure() {
    setName("AppWithProgramStatusSchedule");
    setDescription("Application with program status schedule");
    addWorkflow(new AppWithMultipleWorkflows.SomeWorkflow());
    addWorkflow(new AppWithMultipleWorkflows.AnotherWorkflow());
    Trigger andTrigger = getTriggerFactory().and(
      getTriggerFactory().byTime("*/1 * * * * ?"),
      getTriggerFactory().onProgramStatus(ProgramType.WORKFLOW, TRIGGERING_WORKFLOW, ProgramStatus.COMPLETED));
    schedule(buildSchedule(COMPOSITE_SCHEDULE, ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOn(andTrigger));
  }
}
