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

package co.cask.cdap.longrunning.increment;

import co.cask.cdap.test.TestState;
import com.google.common.base.Objects;

/**
 *
 */
public class IncrementTestState implements TestState {
  private final long sumEvents;
  private final long numEvents;

  public IncrementTestState(long sumEvents, long numEvents) {
    this.sumEvents = sumEvents;
    this.numEvents = numEvents;
  }

  public long getSumEvents() {
    return sumEvents;
  }

  public long getNumEvents() {
    return numEvents;
  }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("sumEvents", sumEvents)
      .add("numEvents", numEvents)
      .toString();
  }
}
