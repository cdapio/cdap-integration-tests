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

package co.cask.cdap.longrunning.logging;

import co.cask.cdap.test.TestState;

/**
 * {@link TestState} used to store time windows for {@link SystemLogsTest}
 */
class SystemLogTestState implements TestState {
  private final Long t1;
  private final Long t2;

  SystemLogTestState(Long t1, Long t2) {
    this.t1 = t1;
    this.t2 = t2;
  }

  Long getT1() {
    return t1;
  }

  Long getT2() {
    return t2;
  }

  @Override
  public String toString() {
    return "SystemLogTestState{" +
      "t1=" + t1 +
      ", t2=" + t2 +
      '}';
  }
}
