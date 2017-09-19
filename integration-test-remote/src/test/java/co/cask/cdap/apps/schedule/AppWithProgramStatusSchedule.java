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
