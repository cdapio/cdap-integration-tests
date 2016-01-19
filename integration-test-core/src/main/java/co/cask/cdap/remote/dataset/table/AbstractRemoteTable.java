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

import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.AbstractTable;
import co.cask.tephra.Transaction;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

/**
 * Override the AbstractTable in CDAP repo, but throwing {@link UnsupportedOperationException} for:
 * TransactionAware methods, createSplitRecordScanner, createSplitReader, scan,
 * because these operations are not supported in the remote implementation of Table.
 */
public abstract class AbstractRemoteTable extends AbstractTable {

  protected AbstractRemoteTable() {
    // fine to pass in null, since those values are only used in the RecordScanner (which this doesn't support)
    super(null, null);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Scanner scan(Scan scan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Split> getSplits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getRecordType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
    throw new UnsupportedOperationException();
  }

  // TransactionAware methods:
  @Override
  public void startTx(Transaction transaction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTx(Transaction transaction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean commitTx() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void postTxCommit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTransactionAwareName() {
    throw new UnsupportedOperationException();
  }
}

