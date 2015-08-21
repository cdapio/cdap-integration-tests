/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ApplicationManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests functionality of Applications
 */
public class ApplicationTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    applicationManager.startFlow("PurchaseFlow");
    getProgramClient().waitForStatus(PurchaseApp.APP_NAME, ProgramType.FLOW, "PurchaseFlow",
                                     "RUNNING", 60, TimeUnit.SECONDS);

    // should not delete application when programs are running
    ApplicationClient appClient = new ApplicationClient(getClientConfig(), getRestClient());
    try {
      appClient.delete(PurchaseApp.APP_NAME);
      Assert.fail();
    } catch (IOException expected) {
      Assert.assertEquals("403: Program is still running", expected.getMessage());
    }

    // should not delete non-existing application
    try {
      appClient.delete("NoSuchApp");
      Assert.fail();
    } catch (ApplicationNotFoundException expected) {
    }
  }
}
