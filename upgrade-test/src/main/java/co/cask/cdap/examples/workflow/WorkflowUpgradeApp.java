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

package co.cask.cdap.examples.workflow;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowToken;

/**
 * Simple Workflow application for testing run record upgrades. Existing application
 * such as PurchaseHistory does not have custom action in it.
 */
public class WorkflowUpgradeApp extends AbstractApplication {

  public static final String NAME = "WorkflowUpgradeApp";
  public static final String WORKFLOW_NAME = "TokenWriterWorkflow";
  public static final String MAPREDUCE_NAME = "TokenWriterMapReduce";
  public static final String CUSTOMACTION_NAME = "TokenWriterCustomAction";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Workflow application to test Workflow run records upgrade.");
    addWorkflow(new TokenWriterWorkflow());
    addMapReduce(new TokenWriterMapReduce());
    createDataset("inputDS", KeyValueTable.class);
    createDataset("outputDS", KeyValueTable.class);
  }

  /**
   * Workflow containing custom action and MapReduce nodes which simply writes
   * some information to the WorkflowToken.
   */
  public static final class TokenWriterWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      addMapReduce(MAPREDUCE_NAME);
      addAction(new TokenWriterCustomAction());
    }
  }

  /**
   * MapReduce program that simply writes information to the WorkflowToken in beforeSubmit method.
   */
  public static final class TokenWriterMapReduce extends AbstractMapReduce {
    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      super.beforeSubmit(context);
      context.setInput("inputDS");
      context.addOutput("outputDS");
      WorkflowToken token = context.getWorkflowToken();
      if (token == null) {
        throw new IllegalStateException("Workflow token cannot be null.");
      }
      token.put("mapreduce.key", "mapreduce.value");
    }
  }

  /**
   * Custom action for writing information to the WorkflowToken.
   */
  public static final class TokenWriterCustomAction extends AbstractWorkflowAction {
    @Override
    public void run() {
      getContext().getToken().put("custom.action.key", "custom.action.value");
    }
  }
}
