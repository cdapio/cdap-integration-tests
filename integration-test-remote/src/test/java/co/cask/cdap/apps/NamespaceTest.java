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

package co.cask.cdap.apps;

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests functionalities of namespaces (create, get, list, delete, etc)
 */
public class NamespaceTest extends AudiTestBase {
  private static final Id.Namespace NS1 = Id.Namespace.from("ns1");
  private static final Id.Namespace NS2 = Id.Namespace.from("ns2");

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
    NamespaceClient namespaceClient = new NamespaceClient(getClientConfig(), getRestClient());

    // initially, there should only be the default namespace
    List<NamespaceMeta> list = namespaceClient.list();
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE_META, list.get(0));

    try {
      namespaceClient.get(NS1.getId());
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (NotFoundException expected) {
//       expected
    }
    try {
      namespaceClient.get(NS2.getId());
      Assert.fail("Expected namespace not to exist: " + NS2);
    } catch (NotFoundException expected) {
//       expected
    }

    // namespace create should work with or without description
    namespaceClient.create(ns1Meta);
    namespaceClient.create(ns2Meta);

    // shouldn't allow creation of default or system namespace
    try {
      namespaceClient.create(Constants.DEFAULT_NAMESPACE_META);
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("'%s' is a reserved namespace",
                                                                     Constants.DEFAULT_NAMESPACE)));
    }

    try {
      namespaceClient.create(new NamespaceMeta.Builder().setName(Constants.SYSTEM_NAMESPACE).build());
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("'%s' is a reserved namespace",
                                                                     Constants.SYSTEM_NAMESPACE)));
    }

    // list should contain the default namespace as well as the two explicitly created
    list = namespaceClient.list();
    Assert.assertEquals(3, list.size());
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
    Assert.assertTrue(list.contains(Constants.DEFAULT_NAMESPACE_META));

    Assert.assertEquals(ns1Meta, namespaceClient.get(NS1.getId()));
    Assert.assertEquals(ns2Meta, namespaceClient.get(NS2.getId()));

    // default namespace should still exist after delete of it
    namespaceClient.delete(Constants.DEFAULT_NAMESPACE);
    namespaceClient.get(Constants.DEFAULT_NAMESPACE);

    // after deleting the explicitly created namespaces, only default namespace should remain in namespace list
    namespaceClient.delete(NS1.getId());
    namespaceClient.delete(NS2.getId());

    list = namespaceClient.list();
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(Constants.DEFAULT_NAMESPACE_META, list.get(0));
  }

  // From a list of NamespaceMeta, finds the element that matches a given namespaceId.
  @Nullable
  private NamespaceMeta getById(List<NamespaceMeta> namespaces, final Id.Namespace namespaceId) {
    Iterable<NamespaceMeta> filter = Iterables.filter(namespaces, new Predicate<NamespaceMeta>() {
      @Override
      public boolean apply(@Nullable NamespaceMeta namespaceMeta) {
        return namespaceMeta != null && namespaceId.getId().equals(namespaceMeta.getName());
      }
    });
    return Iterables.getFirst(filter, null);
  }
}
