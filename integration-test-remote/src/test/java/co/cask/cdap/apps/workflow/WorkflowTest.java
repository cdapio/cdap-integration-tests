/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.apps.workflow;

import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class WorkflowTest extends AudiTestBase {

  @Test
  public void test() throws Exception {

    ApplicationManager applicationManager = deployApplication(WorkflowAppWithFork.class);

    WorkflowManager workflowManager =
      applicationManager.getWorkflowManager(WorkflowAppWithFork.WorkflowWithFork.class.getSimpleName());

    workflowManager.start();

    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    Assert.assertEquals(
      Long.valueOf(
        WorkflowAppWithFork.SimpleActionMain.MAIN_PUT_AMOUNT
          + WorkflowAppWithFork.SimpleActionIncrement.INCREMENT_AMOUNT),
      getTableDataset(WorkflowAppWithFork.DATASET_NAME_ONE).get().get(new Get("row")).getLong("col"));

    Assert.assertEquals(
      Long.valueOf(WorkflowAppWithFork.SimpleActionSecond.BRANCH_PUT_AMOUNT),
      getTableDataset(WorkflowAppWithFork.DATASET_NAME_TWO).get().get(new Get("row")).getLong("col"));
  }

}
