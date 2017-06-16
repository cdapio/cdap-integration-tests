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

package co.cask.cdap.app.resiliency;

import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.chaosmonkey.proto.ClusterDisruptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AdminPersistenceTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    ClusterDisruptor clusterDisruptor = getClusterDisruptor();
    ApplicationManager applicationManager = deployApplication(AdminPersistenceApp.class);

    applicationManager.getWorkerManager("AdminWorker").start();

    ServiceManager serviceManager = applicationManager.getServiceManager("AdminPersistenceService").start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertTrue(applicationManager.getWorkerManager("AdminWorker").isRunning());

    clusterDisruptor.stopAndWait("cdap-master", null, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    clusterDisruptor.restartAndWait("cdap-master", null, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    checkSystemServices();

    Assert.assertTrue(applicationManager.getWorkerManager("AdminWorker").isRunning());
  }
}