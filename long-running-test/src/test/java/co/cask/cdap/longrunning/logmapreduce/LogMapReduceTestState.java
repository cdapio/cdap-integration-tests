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

package co.cask.cdap.longrunning.logmapreduce;

import co.cask.cdap.test.TestState;
import com.google.common.base.Objects;

/**
 * State for LogMapReduceTest
 */
public class LogMapReduceTestState implements TestState {
  private int numRuns;

  public LogMapReduceTestState(int numRuns) {
    this.numRuns = numRuns;
  }

  public int getNumRuns() {
    return numRuns;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("numRuns", numRuns)
      .toString();
  }
}
