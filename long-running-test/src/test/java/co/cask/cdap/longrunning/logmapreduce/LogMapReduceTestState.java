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

  private final long timestamp;
  private final long startTS;
  private final long stopTS;
  private int numBatches;
  private  String runId;

  public LogMapReduceTestState(long timestamp, String runId, long startTS, long stopTS, int numBatches) {
    this.timestamp = timestamp;
    this.runId = runId;
    this.numBatches = numBatches;
    this.startTS = startTS;
    this.stopTS = stopTS;
  }

  public long getTimestamp () {
    return timestamp;
  }

  public long getStartTS () {
    return startTS;
  }

  public long getStopTS () {
    return stopTS;
  }

  public String getRunId() {
    return runId;
  }

  public int getNumBatches() {
    return numBatches;
  }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("runId", runId)
      .add("numBatches", numBatches)
      .toString();
  }
}
