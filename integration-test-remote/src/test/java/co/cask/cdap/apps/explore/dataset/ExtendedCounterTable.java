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

package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;

/**
 *
 */
public interface ExtendedCounterTable extends Dataset, BatchReadable<String, Long>, BatchWritable<String, Long>,
  RecordScannable<ExtendedCounterTable.Count>, RecordWritable<ExtendedCounterTable.Count> {
  void inc(String key, long value);
  long get(String key);

  /**
   *
   */
  class Count {
    private final String word;
    private final long count;
    private final long ts;

    public Count(String word, Long count, Long timestamp) {
      this.word = word;
      this.count = count;
      this.ts = timestamp;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getWord() {
      return word;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getCount() {
      return count;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getTimestamp() {
      return ts;
    }
  }
}
