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

package co.cask.cdap.longrunning.txprune;

import co.cask.cdap.test.TestState;

import java.util.Map;

/**
 *
 */
@SuppressWarnings("WeakerAccess")
public class InvalidListPruneTestState implements TestState {
  private final int iteration;
  // Map of iteration to invalid id generated during that iteration
  private final Map<Integer, Long> invalidTxIds;

  public InvalidListPruneTestState(int iteration, Map<Integer, Long> invalidTxIds) {
    this.iteration = iteration;
    this.invalidTxIds = invalidTxIds;
  }

  public int getIteration() {
    return iteration;
  }

  public Map<Integer, Long> getInvalidTxIds() {
    return invalidTxIds;
  }

  @Override
  public String toString() {
    return "InvalidListPruneTestState{" +
      "iteration=" + iteration +
      ", invalidTxIds=" + invalidTxIds +
      '}';
  }
}
