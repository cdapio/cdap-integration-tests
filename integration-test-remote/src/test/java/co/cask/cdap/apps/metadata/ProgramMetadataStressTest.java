/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.apps.metadata;

import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test which uses {@link ProgramMetadataStressApp} to Stress test metadata from programs
 */
public class ProgramMetadataStressTest extends AudiTestBase {

  private static final ApplicationId APP = TEST_NAMESPACE.app(ProgramMetadataStressApp.APP_NAME);
  private static final ProgramId PROGRAM = APP.program(ProgramType.MAPREDUCE,
                                                       ProgramMetadataStressApp.StressMetadataMR.NAME);

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(ProgramMetadataStressApp.class);
    MapReduceManager mapReduceManager =
      applicationManager.getMapReduceManager(PROGRAM.getProgram());
    mapReduceManager.start();
    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }
}
