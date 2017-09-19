package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ArgumentMapping;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.proto.v2.PluginPropertyMapping;
import co.cask.cdap.etl.proto.v2.TriggeringPropertyMapping;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test property passing between triggering pipeline and triggered pipeline
 */
public class ScheduledPipelinePropertyTest extends ETLTestBase {
  private static final Gson GSON = new Gson();
  private static final String AGE = "age";
  private static final String NAME = "name";

  private static final Schema SOURCE_SCHEMA =
    Schema.recordOf("sourceRecord", Schema.Field.of(AGE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)));


  private static final Schema SINK_SCHEMA =
    Schema.recordOf("sinkRecord", Schema.Field.of(AGE, Schema.of(Schema.Type.STRING)),
                    Schema.Field.of(NAME, Schema.of(Schema.Type.STRING)));

  @Test
  public void testScheduledPipelines() throws Exception {
    // deploy the first pipeline headPipeline
    String headPipeline = "head";
    String headInputTableName = "headInputTable";
    String headOutput = "headOutput";
    ApplicationManager headManager =
      deployValueMapperPipeline(headPipeline, Engine.MAPREDUCE, headInputTableName, String.format("${%s}", headOutput));
    // initialize the input table of head ValueMapper pipeline with AGE 10
    DataSetManager<Table> inputManager = getTableDataset(headInputTableName);
    inputManager.get().put(new Put(Bytes.toBytes("John")).add(AGE, "10"));
    inputManager.flush();
    // deploy the second pipeline middlePipeline which is triggered by the first pipeline headPipeline
    String middlePipeline = "middle";
    String middleInput = "middleInput";
    String middleOutput = "middleOutput";
    String middleOutputFromHead = "middleOutputFromHead";
    ApplicationManager middleManager =
      deployValueMapperPipeline(middlePipeline, Engine.SPARK, String.format("${%s}", middleInput),
                                String.format("${%s}", middleOutput));
    // create a schedule that launches middlePipeline when headPipeline completes.
    // Use the plugin property Properties.BatchReadableWritable.NAME at stage "TableSink" from headPipeline
    // as the value of middleOutput in middlePipeline
    PluginPropertyMapping middlePluginPropertyMapping =
      new PluginPropertyMapping("TableSink", Properties.BatchReadableWritable.NAME, middleInput);
    // Use the runtime argument middleOutputFromHead from headPipeline
    // as the value of runtime argument middleInput in middlePipeline
    ArgumentMapping middleArgumentMapping = new ArgumentMapping(middleOutputFromHead, middleOutput);
    createSchedule(middleManager, middlePipeline, headPipeline,
                   new TriggeringPropertyMapping(ImmutableList.of(middleArgumentMapping),
                                                 ImmutableList.of(middlePluginPropertyMapping)));

    // deploy the third pipeline tailPipeline which is triggered by the first pipeline middlePipeline
    String tailPipeline = "tail";
    String tailInput = "tailInput";
    String tailOutputTableName = "tailOutputTable";
    ApplicationManager tailManager =
      deployValueMapperPipeline(tailPipeline, Engine.MAPREDUCE, String.format("${%s}", tailInput), tailOutputTableName);
    // create a schedule that launches tailPipeline when middlePipeline completes.
    // Use the plugin property Properties.BatchReadableWritable.NAME at stage "TableSink" from middlePipeline
    // as the value of tailInput in tailPipeline
    PluginPropertyMapping tailPluginPropertyMapping =
      new PluginPropertyMapping("TableSink", Properties.BatchReadableWritable.NAME, tailInput);
    createSchedule(tailManager, tailPipeline, middlePipeline,
                   new TriggeringPropertyMapping(ImmutableList.<ArgumentMapping>of(),
                                                 ImmutableList.of(tailPluginPropertyMapping)));

    // run headPipeline with arguments and assert the result
    WorkflowManager headWorkflow = headManager.getWorkflowManager(SmartWorkflow.NAME);
    String headOutputTableName = "headOutputTable";
    String middleOutputTableName = "middleOutputTable";
    headWorkflow.setRuntimeArgs(ImmutableMap.of(headOutput, headOutputTableName,
                                                middleOutputFromHead, middleOutputTableName));
    headWorkflow.start();
    assertPipelineResult(headWorkflow, headOutputTableName);
    // assert result of middlePipeline which should be launched when headPipeline completes
    assertPipelineResult(middleManager.getWorkflowManager(SmartWorkflow.NAME), middleOutputTableName);
    // assert result of tailPipeline which should be launched when middlePipeline completes
    assertPipelineResult(tailManager.getWorkflowManager(SmartWorkflow.NAME), tailOutputTableName);
  }

  private ApplicationManager deployValueMapperPipeline(String pipelineName, Engine engine, String inputTable,
                                                    String outputTable) throws Exception {

    ETLStage source =
      new ETLStage("TableSource", new ETLPlugin("Table",
                                                BatchSource.PLUGIN_TYPE,
                                                ImmutableMap.of(
                                                  Properties.BatchReadableWritable.NAME, inputTable,
                                                  Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "name",
                                                  Properties.Table.PROPERTY_SCHEMA, SOURCE_SCHEMA.toString()), null));
    ETLStage sink =
      new ETLStage("TableSink", new ETLPlugin("Table",
                                              BatchSink.PLUGIN_TYPE,
                                              ImmutableMap.of(
                                                Properties.BatchReadableWritable.NAME, outputTable,
                                                Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "name",
                                                Properties.Table.PROPERTY_SCHEMA, SINK_SCHEMA.toString()), null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> request = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(pipelineName);
    return deployApplication(appId, request);
  }

  private void createSchedule(ApplicationManager triggeredPipeline, String triggeredPipelineName,
                              String triggeringPipelineName, TriggeringPropertyMapping propertyMapping)
    throws Exception {
    // Use properties from the triggering pipeline as values for runtime argument key1, key2
    ProgramStatusTrigger completeTrigger =
      new ProgramStatusTrigger(new WorkflowId(TEST_NAMESPACE.getNamespace(), triggeringPipelineName,
                                              SmartWorkflow.NAME),
                               ImmutableSet.of(ProgramStatus.COMPLETED));
    ScheduleId scheduleId = new ScheduleId(TEST_NAMESPACE.getNamespace(), triggeredPipelineName, "completeSchedule");
    triggeredPipeline.addSchedule(
      new ScheduleDetail(scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(),
                         scheduleId.getSchedule(), "",
                         new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, SmartWorkflow.NAME),
                         ImmutableMap.of(SmartWorkflow.TRIGGERING_PROPERTIES_MAPPING, GSON.toJson(propertyMapping)),
                         completeTrigger, ImmutableList.<Constraint>of(), Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, null));
    triggeredPipeline.getWorkflowManager(SmartWorkflow.NAME).getSchedule(scheduleId.getSchedule()).resume();
  }

  private void assertPipelineResult(WorkflowManager workflowManager, String outputTable)
    throws Exception {
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 10, TimeUnit.MINUTES);
    Table table = getTableDataset(outputTable).get();
    Row row = table.get(Bytes.toBytes("John"));
    Assert.assertEquals("10", row.getString(AGE));
  }
}
