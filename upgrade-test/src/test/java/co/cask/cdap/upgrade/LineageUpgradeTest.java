/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.upgrade;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.LineageClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryBuilder;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.proto.metadata.lineage.RelationRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class LineageUpgradeTest extends UpgradeTestBase {
  private static final NamespaceId LINEAGE_NAMESPACE = new NamespaceId("lineage");
  private static final ApplicationId PURCHASE_APP = LINEAGE_NAMESPACE.app(PurchaseApp.APP_NAME);
  private static final ProgramId PURCHASE_HISTORY_BUILDER = PURCHASE_APP.mr(
    PurchaseHistoryBuilder.class.getSimpleName());
  private static final StreamId PURCHASE_STREAM = LINEAGE_NAMESPACE.stream("purchaseStream");
  private static final StreamViewId PURCHASE_VIEW = PURCHASE_STREAM.view(
    PURCHASE_STREAM.getEntityName() + "View");
  private static final DatasetId HISTORY = LINEAGE_NAMESPACE.dataset("history");
  private static final DatasetId PURCHASES = LINEAGE_NAMESPACE.dataset("purchases");

  private static final String PURCHASE_VIEW_FIELD = "purchaseViewBody";

  @Override
  public void preStage() throws Exception {
    // create lineage test namespace
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(LINEAGE_NAMESPACE).build();
    getNamespaceClient().create(namespaceMeta);

    // deploy an application
    ApplicationManager appManager = deployApplication(LINEAGE_NAMESPACE, PurchaseApp.class);

    // create a view
    Schema viewSchema = Schema.recordOf("record", Schema.Field.of(PURCHASE_VIEW_FIELD,
                                                                  Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StreamViewClient viewClient = new StreamViewClient(getClientConfig(), getRestClient());
    viewClient.createOrUpdate(PURCHASE_VIEW,
                              new ViewSpecification(new FormatSpecification(Formats.AVRO, viewSchema)));

    getStreamClient().sendEvent(PURCHASE_STREAM, "John bought 10 Apples for $1000");
    long startTime = 0;

    MapReduceManager mrManager = appManager.getMapReduceManager(PURCHASE_HISTORY_BUILDER.getProgram()).start();
    mrManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    RunId runId = getRunningProgramRunId(PURCHASE_HISTORY_BUILDER);

    waitForStop(PURCHASE_HISTORY_BUILDER, true);
    long stopTime = TimeMathParser.nowInSeconds();

    LineageRecord lineage = new LineageClient(getClientConfig(), getRestClient())
      .getLineage(PURCHASES, startTime, stopTime, 10);
    LineageRecord expected = getExpectedLineageRecord(stopTime, runId);

    Assert.assertEquals(expected, lineage);
  }
  
  @Override
  public void postStage() throws Exception {
    long startTime = 0;
    long stopTime = TimeMathParser.nowInSeconds();

    LineageRecord lineage = new LineageClient(getClientConfig(), getRestClient())
      .getLineage(PURCHASES, startTime, stopTime, 10);
    Set<RelationRecord> relations = lineage.getRelations();

    // Only concerned about the program access, not the run id, so get it from the query
    Assert.assertFalse(relations.isEmpty());
    Set<String> runs = ((RelationRecord) relations.toArray()[0]).getRuns();
    Assert.assertFalse(relations.isEmpty());
    RunId runId = RunIds.fromString((String) runs.toArray()[0]);

    LineageRecord expected = getExpectedLineageRecord(stopTime, runId);
    Assert.assertEquals(expected, lineage);
  }

  private LineageRecord getExpectedLineageRecord(long stopTime, RunId runId) {
    return LineageSerializer.toLineageRecord(
      0,
      stopTime,
      new Lineage(ImmutableSet.of(
        new Relation(HISTORY, PURCHASE_HISTORY_BUILDER, AccessType.WRITE, runId),
        new Relation(PURCHASES, PURCHASE_HISTORY_BUILDER, AccessType.READ, runId)
      )),
      Collections.<CollapseType>emptySet());
  }

  private RunId getRunningProgramRunId(final ProgramId program) throws Exception {
    waitState(program, ProgramStatus.RUNNING);
    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return getProgramClient().getProgramRuns(program, ProgramStatus.RUNNING.name(), 0,
                                          Long.MAX_VALUE, Integer.MAX_VALUE).isEmpty();
      }
    }, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    List<RunRecord> programRuns = getProgramClient().getProgramRuns(program, ProgramStatus.RUNNING.name(), 0,
                                                                    Long.MAX_VALUE, Integer.MAX_VALUE);
    return RunIds.fromString(programRuns.get(0).getPid());
  }

  private void waitForStop(ProgramId program, boolean needsStop) throws Exception {
    if (needsStop && getProgramClient().getStatus(program).equals(ProgramRunStatus.RUNNING.toString())) {
      LOG.info("Stopping program {}", program);
      getProgramClient().stop(program);
    }
    waitState(program, ProgramStatus.STOPPED);
    LOG.info("Program {} has stopped", program);
  }

  private void waitState(final ProgramId program, ProgramStatus state) throws Exception {
    Tasks.waitFor(state.toString(), new Callable<String>() {
      @Override
      public String call() throws Exception {
        return getProgramClient().getStatus(program);
      }
    }, 60000, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
  }
}
