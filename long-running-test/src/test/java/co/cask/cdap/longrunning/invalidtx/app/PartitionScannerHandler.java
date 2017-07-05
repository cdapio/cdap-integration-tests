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
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 *
 */
public final class PartitionScannerHandler extends AbstractHttpServiceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionScannerHandler.class);

  @GET
  @Path("pfs/{id}")
  public void scanPFS(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("id") String id,
                      @QueryParam("col") @DefaultValue("w") String col,
                      @QueryParam("startTime") Long startTime,
                      @QueryParam("stopTime") Long stopTime,
                      @QueryParam("limit") @DefaultValue("2147483647") int limit,
                      @QueryParam("limit") @DefaultValue("9223372036854775807") long timeoutMillis,
                      @QueryParam("delete") @DefaultValue("false") boolean delete)
    throws NoSuchFieldException, IllegalAccessException {
    Stopwatch sw = new Stopwatch().start();

    byte[] colByte = getColByte(col);
    byte[] startKey = startTime != null ? Bytes.toBytes(startTime) : null;
    byte[] stopKey = stopTime != null ? Bytes.toBytes(stopTime) : null;


    PFSWrapper pfsWrapper = getContext().getDataset(id);
    PartitionedFileSet pfs = pfsWrapper.getPFS();
    IndexedTable indexedTable = get(pfs, "partitionsTable");
    Table idxTable = get(indexedTable, "index");
    Table dataTable = get(indexedTable, "table");
    Scanner scan = indexedTable.scanByIndex(colByte, startKey, stopKey);
    // scan is an IndexedTable#IndexRangeScanner
    IndexScanner ourScanner =
      new IndexScanner((Scanner) get(scan, "baseScanner", true),
                       (byte[]) get(scan, "column", true),
                       (byte[]) get(scan, "start"),
                       (byte[]) get(scan, "end"));

    int rowsProcessed = 0;
    Long lastTimeProcessed = null;
    IndexScanner.IndexRow row;
    List<Row> emptyDataRows = new ArrayList<>();
    while ((row = (IndexScanner.IndexRow) ourScanner.next()) != null
      && rowsProcessed < limit && sw.elapsedMillis() <= timeoutMillis) {

      co.cask.cdap.api.dataset.table.Row dataRow = dataTable.get(row.get(IDX_COL));
      if (!dataRow.getColumns().containsKey(colByte)) {
        String rowKeyString = Bytes.toStringBinary(row.getRow());
        LOG.warn("Data row is missing for key {}. data row columnsSize: {}",
                 rowKeyString, dataRow.getColumns().size());
        emptyDataRows.add(new Row(Bytes.toLong(row.getColValue()), rowKeyString));
        if (delete) {
          LOG.info("Deleting row: {}", rowKeyString);
          idxTable.delete(row.getRow());
        }
      }

      lastTimeProcessed = Bytes.toLong(row.getColValue());
      rowsProcessed++;
    }

    Response response = new Response(row == null, lastTimeProcessed, rowsProcessed, emptyDataRows);
    responder.sendJson(200, response);
  }

  private static final byte[] IDX_COL = {'r'};

  private <T> T get(Object o, String fieldName) throws IllegalAccessException, NoSuchFieldException {
    return get(o, fieldName, false);
  }

  private <T> T get(Object o, String fieldName,
                    boolean definedOnParent) throws IllegalAccessException, NoSuchFieldException {
    Class<?> clz = o.getClass();
    if (definedOnParent) {
      clz = clz.getSuperclass();
    }
    // NoSuchFieldException
    Field field = clz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(o);
  }

  private byte[] getColByte(String col) {
    Preconditions.checkArgument(ImmutableSet.of("w", "c").contains(col)); // results in 500; TODO: turn into 400
    return new byte[]{(byte) col.charAt(0)};
  }

  public static final class Response {
    private final boolean reachedEndOfScan;
    // pass this value as the 'startTime' to the next query. Note that the rows for this time will be scanned again
    private final Long lastTimeProcessed;
    private final int rowsProcessed;
    private final int emptyDataRowCount;
    private final List<Row> emptyDataRows;

    private Response(boolean reachedEndOfScan, Long lastTimeProcessed, int rowsProcessed, List<Row> emptyDataRows) {
      this.reachedEndOfScan = reachedEndOfScan;
      this.lastTimeProcessed = lastTimeProcessed;
      this.rowsProcessed = rowsProcessed;
      this.emptyDataRowCount = emptyDataRows.size();
      this.emptyDataRows = emptyDataRows;
    }

    public boolean isReachedEndOfScan() {
      return reachedEndOfScan;
    }

    public Long getLastTimeProcessed() {
      return lastTimeProcessed;
    }

    public int getRowsProcessed() {
      return rowsProcessed;
    }

    public int getEmptyDataRowCount() {
      return emptyDataRowCount;
    }

    public List<Row> getEmptyDataRows() {
      return emptyDataRows;
    }

    @Override
    public String toString() {
      return "{" +
        "reachedEndOfScan:" + reachedEndOfScan +
        ", lastTimeProcessed:" + lastTimeProcessed +
        ", rowsProcessed:" + rowsProcessed +
        ", emptyDataRowCount:" + emptyDataRowCount +
        ", emptyDataRows:" + emptyDataRows +
        '}';
    }
  }

  private static final class Row {
    private final long timestamp;
    private final String rowKey;

    private Row(long timestamp, String rowKey) {
      this.timestamp = timestamp;
      this.rowKey = rowKey;
    }

    @Override
    public String toString() {
      return timestamp + ":" + rowKey;
    }
  }
}
