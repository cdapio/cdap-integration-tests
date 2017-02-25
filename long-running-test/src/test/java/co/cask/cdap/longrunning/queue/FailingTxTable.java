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

package co.cask.cdap.longrunning.queue;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.tephra.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A special table designed to cause an invalid transaction in CDAP's queue state table when a flag is set.
 */
public class FailingTxTable extends AbstractDataset {
  public static final Logger LOG = LoggerFactory.getLogger(FailingTxTable.class);

  private final Table table;
  private boolean shouldFail;
  private Transaction tx;

  public FailingTxTable(DatasetSpecification spec, @EmbeddedDataset("table") Table table) {
    super(spec.getName(), table);
    this.table = table;
    this.shouldFail = false;
  }

  public void setShouldFail(boolean shouldFail) {
    this.shouldFail = shouldFail;
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public void updateTx(Transaction tx) {
    super.updateTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return super.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    if (shouldFail) {
      LOG.error("going to fail tx {}", tx);
    }
    return super.commitTx();
  }

  @Override
  public void postTxCommit() {
    super.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    if (shouldFail) {
      Runtime.getRuntime().halt(1);
    }
    return super.rollbackTx();
  }
}
