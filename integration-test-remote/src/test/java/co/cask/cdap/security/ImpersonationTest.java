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

package co.cask.cdap.security;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineApp;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 *  Impersonation Tests
 */
public class ImpersonationTest extends AudiTestBase {
  private static final NamespaceId NS1 = new NamespaceId("namespace1");

  @Test
  public void testNamespaceImpersonation() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();

    // initially, only the default namespace should exist
    List<NamespaceMeta> list = namespaceClient.list();
    int initialNamespaceCount = list.size();
    NamespaceMeta defaultMeta = getById(list, NamespaceId.DEFAULT);
    Assert.assertEquals(NamespaceMeta.DEFAULT, defaultMeta);

    try {
      namespaceClient.get(NS1);
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (NamespaceNotFoundException expected) {
      // expected
    }

    // namespace create should work with or without description
    registerForDeletion(NS1);

    NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
      .setName(NS1)
      .setDescription("testDescription")
      .setSchedulerQueueName("testSchedulerQueueName")
      .setPrincipal("alice")
      .setGroupName("admin")
      .setKeytabURI("/etc/security/keytabs/alice.keytab")
      .build();
    namespaceClient.create(ns1Meta);

    // list should contain the default namespace as well as the one explicitly created
    list = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount + 1, list.size());
    Assert.assertTrue(list.contains(ns1Meta));
    NamespaceMeta retrievedNs1Meta = getById(list, NS1);
    Assert.assertNotNull(String.format("Failed to find namespace with name %s in list: %s",
                                       NS1, Joiner.on(", ").join(list)), retrievedNs1Meta);
    Assert.assertEquals(ns1Meta, retrievedNs1Meta);
    Assert.assertTrue(list.contains(NamespaceMeta.DEFAULT));
    Assert.assertEquals(ns1Meta, namespaceClient.get(NS1));

    // Test Streams
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    StreamId STREAM_ID1 = NS1.stream("streamTest1");

    // create properties with user not in the same group as the user who created the namespace
    StreamProperties streamProperties =
      new StreamProperties(1L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                       ImmutableMap.<String, String>of()), 128, null, "eve");
    try {
      streamClient.create(STREAM_ID1, streamProperties);
      Assert.fail("Expected stream creation to fail for this user");
    } catch (IOException expected) {
      expected.printStackTrace();
    }

    // check if stream can be created in the namespace by the owner user
    StreamId STREAM_ID2 = NS1.stream("streamTest2");
    streamProperties = new StreamProperties(1L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                               ImmutableMap.<String, String>of()), 128, null, "alice");
    streamClient.create(STREAM_ID2, streamProperties);
    Assert.assertEquals(streamProperties, streamClient.getConfig(STREAM_ID2));


    // Test Datasets
    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());
    DatasetId testDatasetInstance1 = NS1.dataset("testDataset1");

    try {
      datasetClient.create(testDatasetInstance1, new DatasetInstanceConfiguration("table", null, null, "eve"));
      Assert.fail("Expected dataset creation to fail for this user");
    } catch (Exception expected) {
      expected.printStackTrace();
    }

    DatasetId testDatasetInstance2 = NS1.dataset("testDataset2");
    datasetClient.create(testDatasetInstance2, new DatasetInstanceConfiguration("table", null, null, "alice"));

    // Verify owner was able to create the dataset
    Assert.assertTrue(datasetClient.exists(testDatasetInstance2));

    // Test Apps
    ArtifactId artifactId = NS1.artifact("WikipediaPipelineArtifact", "1.0.0");
    getTestManager().addAppArtifact(artifactId, WikipediaPipelineApp.class);

    ArtifactSummary artifactSummary =  new ArtifactSummary("WikipediaPipelineArtifact", "1.0.0");
    ApplicationId applicationId = NS1.app(WikipediaPipelineApp.class.getSimpleName());

    // deploy should fail when trying with user not in the same group as the owner
    try {
      deployApplication(applicationId, new AppRequest(artifactSummary, null, "eve"));
      Assert.fail("Expected deploy app to fail for this user");
    } catch (Exception expected) {
      expected.printStackTrace();
    }

    // Check if application can be deployed by onwer
    deployApplication(applicationId, new AppRequest(artifactSummary, null, "alice"));

    // after deleting the explicitly created namespaces, only default namespace should remain in namespace list
    namespaceClient.delete(NS1);

    list = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount, list.size());
    defaultMeta = getById(list, NamespaceId.DEFAULT);
    Assert.assertEquals(NamespaceMeta.DEFAULT, defaultMeta);
  }

  // From a list of NamespaceMeta, finds the element that matches a given namespaceId.
  @Nullable
  private NamespaceMeta getById(List<NamespaceMeta> namespaces, final NamespaceId namespaceId) {
    Iterable<NamespaceMeta> filter = Iterables.filter(namespaces, new Predicate<NamespaceMeta>() {
      @Override
      public boolean apply(@Nullable NamespaceMeta namespaceMeta) {
        return namespaceMeta != null && namespaceId.getNamespace().equals(namespaceMeta.getName());
      }
    });
    return Iterables.getFirst(filter, null);
  }
}
