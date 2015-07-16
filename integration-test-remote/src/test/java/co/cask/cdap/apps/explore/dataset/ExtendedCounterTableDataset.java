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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.batch.SplitReaderAdapter;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * ExtendedCounterTableDataset
 */
public class ExtendedCounterTableDataset extends AbstractDataset implements ExtendedCounterTable {

  private static final String COL = "";

  private final Table table;

  public ExtendedCounterTableDataset(String name, Table table) {
    super(name, table);
    this.table = table;
  }

  public void inc(String key, long value) {
    table.increment(new Increment(key, COL, value));
  }

  public long get(String key) {
    return table.get(new Get(key, COL)).getLong(COL, 0);
  }

  // Batch Support

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public SplitReader<String, Long> createSplitReader(Split split) {
    return new SplitReaderAdapter<byte[], String, Row, Long>(table.createSplitReader(split)) {
      @Override
      protected String convertKey(byte[] key) {
        return Bytes.toString(key);
      }

      @Override
      protected Long convertValue(Row value) {
        return value.getLong(COL, 0);
      }
    };
  }

  @Override
  public void write(Count record) throws IOException {
    write(record.getWord(), record.getCount());
  }

  @Override
  public void write(String key, Long value) {
    inc(key, value);
  }

  @Override
  public Type getRecordType() {
    return Count.class;
  }

  @Override
  public RecordScanner<Count> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(createSplitReader(split), COUNT_ROW_MAKER);
  }

  private static final Scannables.RecordMaker<String, Long, Count> COUNT_ROW_MAKER =
    new Scannables.RecordMaker<String, Long, Count>() {
      @Override
      public Count makeRecord(String s, Long aLong) {
        return new Count(s, aLong, System.currentTimeMillis());
      }
    };
}
