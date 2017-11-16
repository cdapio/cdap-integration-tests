/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.apps.purchase;

import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.examples.purchase.CatalogLookupService;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.StreamManager;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the functionality demonstrated in PurchaseApp
 */
public class PurchaseAudiTest extends AudiTestBase {
  private static final ApplicationId PURCHASE_APP = TEST_NAMESPACE.app(PurchaseApp.APP_NAME);
  private static final FlowId PURCHASE_FLOW = PURCHASE_APP.flow("PurchaseFlow");

  @Override
  public void setUp() throws Exception {
    checkSystemServices();
//    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    // avoid tearDown
//    super.tearDown();
  }

  @Test
  public void test() throws Exception {
    // start PurchaseFlow and ingest an event
    getflowManager();

    StreamManager purchaseStream = getTestManager().getStreamManager(TEST_NAMESPACE.stream("purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");


    while (Math.random() < 1) {
      purchaseStream.send("Milo bought 10 PBR for $12");
//      TimeUnit.SECONDS.sleep(1);
    }
  }

  private FlowManager getflowManager() throws Exception {
    ApplicationId purchaseHistory = getConfiguredNamespace().app("PurchaseHistory");
    try {
      // check if it already exists
      getApplicationClient().get(purchaseHistory);
      ApplicationManager appManager = getApplicationManager(purchaseHistory);
      return appManager.getFlowManager(PURCHASE_FLOW.getProgram());
    } catch (ApplicationNotFoundException e) {
      ApplicationManager appManager = deployApplication(PurchaseApp.class);
      FlowManager flowManager = appManager.getFlowManager(PURCHASE_FLOW.getProgram()).start();
      flowManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      appManager.getServiceManager(CatalogLookupService.SERVICE_NAME).start();

      return flowManager;
    }
  }
}
