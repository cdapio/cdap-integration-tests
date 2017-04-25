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

package co.cask.cdap.app.resiliency.plugins;

import co.cask.cdap.test.DisruptionTestBase;
import co.cask.chaosmonkey.proto.ClusterDisruptor;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MajorCompactionTest extends DisruptionTestBase {

  @Test
  public void test() throws Exception {
    ClusterDisruptor clusterDisruptor = getClusterDisruptor();
    clusterDisruptor.disruptAndWait("hbase-master", "major-compact", null, PROGRAM_START_STOP_TIMEOUT_SECONDS,
                                    TimeUnit.SECONDS);
  }
}
