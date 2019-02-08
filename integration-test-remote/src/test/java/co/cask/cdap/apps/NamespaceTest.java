/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.apps;

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests functionality of namespaces (create, get, list, delete, etc)
 */
public class NamespaceTest extends AudiTestBase {
  private static final NamespaceId NS1 = new NamespaceId("ns1");
  private static final NamespaceId NS2 = new NamespaceId("ns2");

  private static final NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
    .setName(NS1)
    .setDescription("testDescription")
    .setSchedulerQueueName("testSchedulerQueueName")
    .build();
  private static final NamespaceMeta ns2Meta = new NamespaceMeta.Builder()
    .setName(NS2)
    .build();

  @Test
  public void testNamespaces() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();

    // initially, there should be at least the default namespace
    List<NamespaceMeta> list = namespaceClient.list();
    int initialNamespaceCount = list.size();
    NamespaceMeta defaultMeta = getById(list, NamespaceId.DEFAULT);
    Assert.assertNotNull(defaultMeta);
    Assert.assertEquals(NamespaceMeta.DEFAULT, new NamespaceMeta.Builder(defaultMeta).setGeneration(0L).build());

    try {
      namespaceClient.get(NS1);
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (NamespaceNotFoundException expected) {
      // expected
    }
    try {
      namespaceClient.get(NS2);
      Assert.fail("Expected namespace not to exist: " + NS2);
    } catch (NamespaceNotFoundException expected) {
      // expected
    }

    // namespace create should work with or without description
    registerForDeletion(NS1, NS2);
    namespaceClient.create(ns1Meta);
    namespaceClient.create(ns2Meta);

    // shouldn't allow creation of default or system namespace
    try {
      namespaceClient.create(NamespaceMeta.DEFAULT);
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("'%s' is a reserved namespace",
                                                                     NamespaceId.DEFAULT.getNamespace())));
    }

    try {
      namespaceClient.create(new NamespaceMeta.Builder().setName(NamespaceId.SYSTEM).build());
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("'%s' is a reserved namespace",
                                                                     NamespaceId.SYSTEM.getNamespace())));
    }

    // zero out generation so that it's not used in comparison
    list.clear();
    for (NamespaceMeta ns : namespaceClient.list()) {
      list.add(new NamespaceMeta.Builder(ns).setGeneration(0L).build());
    }

    // list should contain the default namespace as well as the two explicitly created
    Assert.assertEquals(initialNamespaceCount + 2, list.size());
    Assert.assertTrue(list.contains(ns1Meta));
    NamespaceMeta retrievedNs1Meta = getById(list, NS1);
    Assert.assertNotNull(String.format("Failed to find namespace with name %s in list: %s",
                                       NS1, Joiner.on(", ").join(list)),
                         retrievedNs1Meta);
    Assert.assertEquals(ns1Meta, retrievedNs1Meta);
    Assert.assertTrue(list.contains(ns2Meta));
    NamespaceMeta retrievedNs2Meta = getById(list, NS2);
    Assert.assertNotNull(String.format("Failed to find namespace with name %s in list: %s",
                                       NS2, Joiner.on(", ").join(list)),
                         retrievedNs1Meta);
    Assert.assertEquals(ns2Meta, retrievedNs2Meta);
    Assert.assertTrue(list.contains(NamespaceMeta.DEFAULT));

    Assert.assertEquals(ns1Meta, new NamespaceMeta.Builder(namespaceClient.get(NS1)).setGeneration(0L).build());
    Assert.assertEquals(ns2Meta, new NamespaceMeta.Builder(namespaceClient.get(NS2)).setGeneration(0L).build());

    // after deleting the explicitly created namespaces, only default namespace should remain in namespace list
    namespaceClient.delete(NS1);
    namespaceClient.delete(NS2);

    list = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount, list.size());
    defaultMeta = getById(list, NamespaceId.DEFAULT);
    Assert.assertNotNull(defaultMeta);
    Assert.assertEquals(NamespaceMeta.DEFAULT, new NamespaceMeta.Builder(defaultMeta).setGeneration(0L).build());
  }

  // From a list of NamespaceMeta, finds the element that matches a given namespaceId.
  @Nullable
  private NamespaceMeta getById(List<NamespaceMeta> namespaces, final NamespaceId namespaceId) {
    Iterable<NamespaceMeta> filter = Iterables.filter(namespaces, namespaceMeta ->
      namespaceMeta != null && namespaceId.getNamespace().equals(namespaceMeta.getName()));
    return Iterables.getFirst(filter, null);
  }
}
