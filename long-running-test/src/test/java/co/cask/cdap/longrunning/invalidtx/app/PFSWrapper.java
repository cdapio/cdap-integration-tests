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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;

/**
 * A wrapper around {@link PartitionedFileSet}, which ensures that transaction rollback will fail.
 */
public class PFSWrapper extends AbstractDataset {
  private final PartitionedFileSet pfs;

  public PFSWrapper(DatasetSpecification spec,
                    @EmbeddedDataset("pfs") PartitionedFileSet pfs) {
    super(spec.getName(), pfs);
    this.pfs = pfs;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    throw new Exception("Forcing failure of rollbackTx.");
  }

  public PartitionOutput getPartitionOutput(PartitionKey key) {
    return pfs.getPartitionOutput(key);
  }

  public Partition getPartition(PartitionKey key) {
    return pfs.getPartition(key);
  }

  public PartitionedFileSet getPFS() {
    return pfs;
  }
}
