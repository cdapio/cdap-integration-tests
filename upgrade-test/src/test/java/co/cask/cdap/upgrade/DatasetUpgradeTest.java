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

package co.cask.cdap.upgrade;

import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.examples.appwithsleepingflowlet.AppWithSleepingFlowlet;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests that application spec, run records, schedule information can be retrieved after an upgrade.
 */
public class DatasetUpgradeTest extends UpgradeTestBase {
  private static final Gson GSON = new Gson();
  private static final Type STRING_BOOLEAN_TYPE = new TypeToken<Map<String, Boolean>>() { }.getType();

  @Override
  protected void preStage() throws Exception {
    List<ApplicationRecord> appList = getApplicationClient().list(getConfiguredNamespace());
    Assert.assertFalse(appList.isEmpty());
    verifySchedule();
  }

  @Override
  protected void postStage() throws Exception {
    final List<ApplicationRecord> expectedAppList = getApplicationClient().list(getConfiguredNamespace());
    verifySchedule();
    verifyAppRecord(expectedAppList);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        verifySchedule();
        verifyAppRecord(expectedAppList);
        HttpResponse response = getRestClient().execute(HttpRequest.get(
          getClientConfig().resolveURLV3("/system/upgrade/status")).build(), getAccessToken());
        Map<String, Boolean> upgradeStatus = GSON.fromJson(response.getResponseBodyAsString(), STRING_BOOLEAN_TYPE);
        boolean upgradeFlag = true;
        for (Map.Entry<String, Boolean> entry : upgradeStatus.entrySet()) {
          upgradeFlag &= entry.getValue();
        }
        return upgradeFlag;
      }
    }, 10, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);

    verifySchedule();
    verifyAppRecord(expectedAppList);
  }

  private void verifyAppRecord(List<ApplicationRecord> expectedRecordList) throws Exception {
    List<ApplicationRecord> appList = getApplicationClient().list(getConfiguredNamespace());
    Assert.assertEquals(expectedRecordList.size(), appList.size());
    Assert.assertTrue(expectedRecordList.containsAll(appList));
  }

  private void verifySchedule() throws Exception {
    WorkflowManager workflowManager = getApplicationManager(getConfiguredNamespace().app(AppWithSleepingFlowlet.NAME))
      .getWorkflowManager(AppWithSleepingFlowlet.DummyWorkflow.NAME);
    Assert.assertFalse(workflowManager.getSchedules().isEmpty());
  }
}
