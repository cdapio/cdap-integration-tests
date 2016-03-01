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

package co.cask.cdap.longrunning.datacleansing;

import co.cask.cdap.test.TestState;
import com.google.common.base.Objects;

/**
 * State for DataCleansingTest
 */
public class DataCleansingTestState implements TestState {

  private final long timestamp;
  private final long startCleanRecordPid;
  private final long endCleanRecordPid;
  private final long startInvalidRecordPid;
  private final long endInvalidRecordPid;

  public DataCleansingTestState(long timestamp, long startCleanRecordPid, long endCleanRecordPid,
                                long startInvalidRecordPid, long endInvalidRecordPid) {
    this.timestamp = timestamp;
    this.startCleanRecordPid = startCleanRecordPid;
    this.endCleanRecordPid = endCleanRecordPid;
    this.startInvalidRecordPid = startInvalidRecordPid;
    this.endInvalidRecordPid = endInvalidRecordPid;
  }

  public long getTimestamp () {
    return timestamp;
  }

  public long getStartCleanRecordPid() {
    return startCleanRecordPid;
  }

  public long getEndCleanRecordPid() {
    return endCleanRecordPid;
  }

  public long getStartInvalidRecordPid() {
    return startInvalidRecordPid;
  }

  public long getEndInvalidRecordPid() {
    return endInvalidRecordPid;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("startCleanRecordPid", startCleanRecordPid)
      .add("endCleanRecordPid", endCleanRecordPid)
      .add("startInvalidRecordPid", startInvalidRecordPid)
      .add("endInvalidRecordPid", endInvalidRecordPid)
      .toString();
  }
}
