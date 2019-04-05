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

package io.cdap.cdap.remote.dataset.test;

import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.DataSetManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests {@link io.cdap.cdap.remote.dataset.kvtable.RemoteKeyValueTable}
 */
public class RemoteKeyValueTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    DataSetManager<KeyValueTable> kvTableManager = getKVTableDataset("kvTable");
    KeyValueTable kvTable = kvTableManager.get();

    Assert.assertEquals(null, kvTable.read("k"));

    kvTable.write("k", "v");
    Assert.assertArrayEquals("v".getBytes(), kvTable.read("k".getBytes()));
    Assert.assertArrayEquals("v".getBytes(), kvTable.read("k"));

    byte[] newValueBytes = "newValue".getBytes();
    Assert.assertFalse(kvTable.compareAndSwap("k".getBytes(), "c".getBytes(), newValueBytes));
    Assert.assertTrue(kvTable.compareAndSwap("k".getBytes(), "v".getBytes(), newValueBytes));
    Assert.assertArrayEquals(newValueBytes, kvTable.read("k"));


    byte[] a = "a".getBytes();
    byte[] b = "b".getBytes();
    byte[] c = "c".getBytes();

    kvTable.write(a, b);
    Map<byte[], byte[]> results = kvTable.readAll(new byte[][]{ a, b, c });
    Assert.assertArrayEquals(b, results.get(a));
    Assert.assertEquals(null, results.get(b));
    Assert.assertEquals(null, results.get(c));

    kvTable.delete(a);
    Assert.assertEquals(null, kvTable.read(a));
  }
}
