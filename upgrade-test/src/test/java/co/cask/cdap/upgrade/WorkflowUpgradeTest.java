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

import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.examples.workflow.WorkflowUpgradeApp;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests upgrading of the Workflow run records.
 */
public class WorkflowUpgradeTest extends UpgradeTestBase {

  private static final String WORKFLOW_TOKEN_PROPERTY_NAME = "workflowToken";

  @Override
  protected void preStage() throws Exception {
    ApplicationManager appManager = deployApplication(WorkflowUpgradeApp.class);
    WorkflowManager workflowManager = appManager.getWorkflowManager(WorkflowUpgradeApp.WORKFLOW_NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // Verify one run of Workflow is completed
    List<RunRecord> history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());
    String workflowRunId = history.get(0).getPid();
    verifyWorkflowToken(workflowManager, workflowRunId);
  }

  @Override
  protected void postStage() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app(WorkflowUpgradeApp.NAME);
    Assert.assertTrue("WorkflowUpgradeApp must exist after upgrade.", getApplicationClient().exists(appId.toId()));

    WorkflowManager workflowManager = getApplicationManager(appId).getWorkflowManager(WorkflowUpgradeApp.WORKFLOW_NAME);

    // Verify the Workflow had one run in pre-stage
    List<RunRecord> history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());

    // Verify workflow token for the run started before upgrade
    String workflowRunIdBeforeUpgrade = history.get(0).getPid();
    verifyWorkflowToken(workflowManager, workflowRunIdBeforeUpgrade);

    MapReduceManager mapReduceManager = getApplicationManager(appId)
      .getMapReduceManager(WorkflowUpgradeApp.MAPREDUCE_NAME);

    history = mapReduceManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, history.size());

    String mapReduceRunIdBeforeUpgrade = history.get(0).getPid();
    verifyWorkflowNodeStates(workflowManager, workflowRunIdBeforeUpgrade, mapReduceRunIdBeforeUpgrade, false);

    // Start new run of the Workflow
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    // Should be two runs of the Workflow
    history = workflowManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(2, history.size());

    String workflowRunIdAfterUpgrade = null;
    for (RunRecord record : history) {
      if (record.getPid().equals(workflowRunIdBeforeUpgrade)) {
        Assert.assertTrue(record.getProperties().containsKey(WORKFLOW_TOKEN_PROPERTY_NAME));
        continue;
      }
      workflowRunIdAfterUpgrade = record.getPid();
      Assert.assertFalse(record.getProperties().containsKey(WORKFLOW_TOKEN_PROPERTY_NAME));
    }

    Assert.assertNotNull(workflowRunIdAfterUpgrade);
    Assert.assertNotEquals(workflowRunIdBeforeUpgrade, workflowRunIdAfterUpgrade);

    history = mapReduceManager.getHistory(ProgramRunStatus.COMPLETED);
    Assert.assertEquals(2, history.size());

    String mapReduceRunIdAfterUpgrade = null;
    for (RunRecord record : history) {
      if (record.getPid().equals(mapReduceRunIdBeforeUpgrade)) {
        continue;
      }
      mapReduceRunIdAfterUpgrade = record.getPid();
    }

    // Verify workflow token started after upgrade
    verifyWorkflowToken(workflowManager, workflowRunIdAfterUpgrade);
    verifyWorkflowNodeStates(workflowManager, workflowRunIdAfterUpgrade, mapReduceRunIdAfterUpgrade, true);
  }

  private void verifyWorkflowToken(WorkflowManager workflowManager, String runId) throws Exception {
    WorkflowTokenDetail token = workflowManager.getToken(runId, null, null);

    // Two different keys were added to the token
    Assert.assertEquals(2, token.getTokenData().size());

    List<WorkflowTokenDetail.NodeValueDetail> nodeValueDetails = token.getTokenData().get("mapreduce.key");
    Assert.assertEquals(1, nodeValueDetails.size());
    Assert.assertEquals(WorkflowUpgradeApp.MAPREDUCE_NAME, nodeValueDetails.get(0).getNode());
    Assert.assertEquals("mapreduce.value", nodeValueDetails.get(0).getValue());

    nodeValueDetails = token.getTokenData().get("custom.action.key");
    Assert.assertEquals(1, nodeValueDetails.size());
    Assert.assertEquals(WorkflowUpgradeApp.CUSTOMACTION_NAME, nodeValueDetails.get(0).getNode());
    Assert.assertEquals("custom.action.value", nodeValueDetails.get(0).getValue());
  }

  private void verifyWorkflowNodeStates(WorkflowManager workflowManager, String workflowRunId,
                                        String mapReduceRunId, boolean postStageRun) throws Exception {
    Map<String, WorkflowNodeStateDetail> workflowNodeStates = workflowManager.getWorkflowNodeStates(workflowRunId);
    // Pre 3.4 runs will only have node state for map reduce after upgrade and not for custom action
    Assert.assertEquals(postStageRun ? 2 : 1, workflowNodeStates.size());
    Assert.assertEquals(WorkflowUpgradeApp.MAPREDUCE_NAME,
                        workflowNodeStates.get(WorkflowUpgradeApp.MAPREDUCE_NAME).getNodeId());
    Assert.assertEquals(NodeStatus.COMPLETED,
                        workflowNodeStates.get(WorkflowUpgradeApp.MAPREDUCE_NAME).getNodeStatus());
    Assert.assertEquals(mapReduceRunId, workflowNodeStates.get(WorkflowUpgradeApp.MAPREDUCE_NAME).getRunId());

    if (postStageRun) {
      // For post stage run custom action node state should be there
      Assert.assertEquals(WorkflowUpgradeApp.CUSTOMACTION_NAME,
                          workflowNodeStates.get(WorkflowUpgradeApp.CUSTOMACTION_NAME).getNodeId());
      Assert.assertEquals(NodeStatus.COMPLETED,
                          workflowNodeStates.get(WorkflowUpgradeApp.CUSTOMACTION_NAME).getNodeStatus());
    }
  }
}
