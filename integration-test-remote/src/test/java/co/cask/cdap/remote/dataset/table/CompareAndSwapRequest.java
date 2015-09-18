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

package co.cask.cdap.remote.dataset.table;

/**
 * CompareAndSwap arguments for {@link co.cask.cdap.api.dataset.table.Table}.
 */
public final class CompareAndSwapRequest {
  private final byte[] row;
  private final byte[] column;
  private final byte[] oldValue;
  private final byte[] newValue;

  public CompareAndSwapRequest(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    this.row = row;
    this.column = column;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getColumn() {
    return column;
  }

  public byte[] getOldValue() {
    return oldValue;
  }

  public byte[] getNewValue() {
    return newValue;
  }
}
