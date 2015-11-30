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

package co.cask.cdap.remote.dataset.test;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.DataSetManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link co.cask.cdap.remote.dataset.table.RemoteTable}.
 */
public class RemoteTableTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    DataSetManager<Table> tableManager = getTableDataset("table");
    Table table = tableManager.get();

    byte[] a = "a".getBytes();
    byte[] b = "b".getBytes();
    byte[] c = "c".getBytes();

    Row row = table.get(a);
    Assert.assertTrue(row.isEmpty());

    table.put(a, b, c);
    row = table.get(a);
    Assert.assertEquals(1, row.getColumns().size());
    Assert.assertArrayEquals(c, row.get(b));

    table.delete(a, b);
    row = table.get(a);
    Assert.assertTrue(row.isEmpty());
  }
}
