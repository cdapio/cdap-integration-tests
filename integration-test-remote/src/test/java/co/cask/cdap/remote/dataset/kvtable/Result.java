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

package co.cask.cdap.remote.dataset.kvtable;

/**
 * Wrapper class around {@code byte[]} for GSON serialization, in case the byte[] is null.
 */
public final class Result {
  private final byte[] result;

  public Result(byte[] result) {
    this.result = result;
  }

  public byte[] getResult() {
    return result;
  }
}
