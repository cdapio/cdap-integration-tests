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

package co.cask.cdap.longrunning.invalidtx.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import javax.annotation.Nullable;

/**
 *
 */
public class IndexScanner implements Scanner {

  private static final Logger LOG = LoggerFactory.getLogger(IndexScanner.class);

  private static final byte[] IDX_COL = {'r'};

  // scanner over index table
  private final Scanner baseScanner;
  private final byte[] column;
  private final byte[] start;
  private final byte[] end;

  IndexScanner(Scanner baseScanner, byte[] column, @Nullable byte[] start, @Nullable byte[] end) {
    this.start = start;
    this.end = end;
    this.baseScanner = baseScanner;
    this.column = column;
  }

  /**
   * checks if a particular column value matches a criteria defined by the implementing class
   *
   * @param columnValue the column to check for a match
   * @return false to indicate to skip the corresponding row
   */
  private boolean matches(byte[] columnValue) {
    return (start == null || Bytes.compareTo(columnValue, start) >= 0)
      && (end == null || Bytes.compareTo(columnValue, end) < 0);
  }

  @Nullable
  @Override
  public Row next() {
    for (Row indexRow = baseScanner.next(); indexRow != null; indexRow = baseScanner.next()) {
      byte[] rowkey = indexRow.get(IDX_COL);
      if (rowkey == null) {
        LOG.warn("Row of Indexed table '{}' is missing index column. Row key: {}", "getName()", indexRow.getRow());
        continue;
      }
      byte[] columnValue = Arrays.copyOfRange(indexRow.getRow(),
                                              column.length + 1,
                                              indexRow.getRow().length - rowkey.length - 1);
      // Verify that datarow matches the expected row key to avoid issues with column name or value
      // containing the delimiter used. This is a sufficient check, as long as columns don't contain the null byte.
      if (matches(columnValue)) {
        return new IndexRow(indexRow, columnValue);
      }
    }
    // end of index
    return null;
  }

  @Override
  public void close() {
    baseScanner.close();
  }

  static final class IndexRow extends Result {
    private final byte[] colValue;

    IndexRow(Row row, byte[] colValue) {
      super(row.getRow(), row.getColumns());
      this.colValue = colValue;
    }

    byte[] getColValue() {
      return colValue;
    }
  }
}
