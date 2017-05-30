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

import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.examples.datacleansing.DataCleansing;
import co.cask.cdap.examples.datacleansing.DataCleansingService;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class ScheduleTest extends AudiTestBase {
  @Test
  public void testDatasetPartitionSchedule() throws Exception {
    // Deploy DataCleansing app and start service
    ApplicationManager dataCleansing = deployApplication(DataCleansing.class);
    ServiceManager serviceManager = dataCleansing.getServiceManager(DataCleansingService.NAME).start();
    // Deploy AppWithDataPartitionSchedule
    ApplicationManager appWithSchedule = deployApplication(AppWithDataPartitionSchedule.class);
    WorkflowManager workflowManager = appWithSchedule.getWorkflowManager(AppWithDataPartitionSchedule.SOME_WORKFLOW);

    ScheduleId scheduleId = TEST_NAMESPACE.app(AppWithDataPartitionSchedule.NAME)
      .schedule(AppWithDataPartitionSchedule.DATASET_PARTITION_SCHEDULE_1);
    new ScheduleClient(getClientConfig(), getRestClient()).resume(scheduleId);

    // Workflow in AppWithDataPartitionSchedule has not been run before
    Assert.assertEquals(0, workflowManager.getHistory().size());
    // Create TRIGGER_ON_NUM_PARTITIONS new partitions with the service in DataCleansing app
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    for (int i = 0; i < AppWithDataPartitionSchedule.TRIGGER_ON_NUM_PARTITIONS; i++) {
      createPartition(serviceURL);
    }
    // Schedule in AppWithDataPartitionSchedule was triggered by TRIGGER_ON_NUM_PARTITIONS new partitions
    // and launched the workflow. Wait for the workflow to complete
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);
  }

  private void createPartition(URL serviceUrl)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    HttpRequest request = HttpRequest.post(url).withBody("new partition").build();
    HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }
}
