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

package co.cask.cdap.longrunning.increment;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;

import java.io.StringWriter;

/**
 *
 */
public class IncrementTest extends LongRunningTestBase<IncrementTestState> {
  private static final int BATCH_SIZE = 100;
  public static final int SUM_BATCH = (BATCH_SIZE * (BATCH_SIZE - 1)) / 2;

  @Override
  public void setup() throws Exception {
    ApplicationManager applicationManager = deployApplication(getLongRunningNamespace(), IncrementApp.class);
    applicationManager.getFlowManager(IncrementApp.IncrementFlow.NAME).start();
  }

  @Override
  public void cleanup() throws Exception {
    // Nothing to do for now
  }

  @Override
  public IncrementTestState getInitialState() {
    return new IncrementTestState(0, 0);
  }

  @Override
  public void verifyRuns(IncrementTestState state) throws Exception {
    DatasetId readlessTableId = new DatasetId(getLongRunningNamespace().getId(), IncrementApp.READLESS_TABLE);
    KeyValueTable readlessTable = getKVTableDataset(readlessTableId).get();
    long readlessSum = readLong(readlessTable.read(IncrementApp.SUM_KEY));
    long readlessNum = readLong(readlessTable.read(IncrementApp.NUM_KEY));
    Assert.assertEquals(state.getSumEvents(), readlessSum);
    Assert.assertEquals(state.getNumEvents(), readlessNum);

    DatasetId regularTableId = new DatasetId(getLongRunningNamespace().getId(), IncrementApp.REGULAR_TABLE);
    KeyValueTable regularTable = getKVTableDataset(regularTableId).get();
    long regularSum = readLong(regularTable.read(IncrementApp.SUM_KEY));
    long regularNum = readLong(regularTable.read(IncrementApp.NUM_KEY));
    Assert.assertEquals(state.getSumEvents(), regularSum);
    Assert.assertEquals(state.getNumEvents(), regularNum);
  }

  @Override
  public IncrementTestState runOperations(IncrementTestState state) throws Exception {
    StreamClient streamClient = getStreamClient();
    LOG.info("Writing {} events in one batch", BATCH_SIZE);
    StringWriter writer = new StringWriter();
    for (int i = 0; i < BATCH_SIZE; i++) {
      writer.write(String.format("%010d", i));
      writer.write("\n");
    }
    streamClient.sendBatch(Id.Stream.from(getLongRunningNamespace(), IncrementApp.INT_STREAM), "text/plain",
                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));
    long newSum = state.getSumEvents() + SUM_BATCH;
    return new IncrementTestState(newSum, state.getNumEvents() + BATCH_SIZE);
  }

  private long readLong(byte[] bytes) {
    return bytes == null ? 0 : Bytes.toLong(bytes);
  }
}
