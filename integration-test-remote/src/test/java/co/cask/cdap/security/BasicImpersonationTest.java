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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.examples.wikipedia.WikipediaPipelineApp;
import co.cask.cdap.proto.ConfigEntry;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 *  Impersonation Tests
 */
public class BasicImpersonationTest extends AudiTestBase {
  private static final NamespaceId NS1 = new NamespaceId("nsbasic");
  private static final String ALICE = "alice";
  private static final String EVE = "eve";
  private static final String BOB = "bob";

  private static final String GROUP_NSCREATOR = "nscreator";

  @Test
  public void testBasicNamespaceImpersonation() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    try {
      namespaceClient.get(NS1);
      Assert.fail("Expected namespace not to exist: " + NS1);
    } catch (NamespaceNotFoundException expected) {
      // expected
    }
    registerForDeletion(NS1);

    NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
      .setName(NS1)
      .setPrincipal(ALICE)
      .setGroupName(GROUP_NSCREATOR)
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns1Meta);
    Assert.assertTrue(namespaceClient.exists(NS1));

    NamespaceMeta retrievedNs1Meta = namespaceClient.get(NS1);
    Assert.assertEquals(ns1Meta, retrievedNs1Meta);

    // Test Stream creation with impersonated namespace
    StreamClient streamClient = new StreamClient(getClientConfig(), getRestClient());
    StreamId streamId = NS1.stream("streamTest");

    // create properties with user not in the same group as the user who created the namespace
    StreamProperties streamProperties =
      new StreamProperties(0L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                       ImmutableMap.<String, String>of()), 128, null, EVE);
    try {
      streamClient.create(streamId, streamProperties);
      Assert.fail("Expected stream creation to fail for this user");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Failed to create directory at")));
      try {
        streamClient.getConfig(streamId);
      } catch (Exception ioe) {
        Assert.assertTrue(ioe.getMessage().contains(String.format("was not found")));
      }
    }

    // check if stream can be created in the namespace by the owner user
    streamProperties = new StreamProperties(1L, new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                               ImmutableMap.<String, String>of()), 128, null, ALICE);

    streamClient.create(streamId, streamProperties);
    Assert.assertEquals(streamProperties, streamClient.getConfig(streamId));

    // check if user in the same group as owner can also create a stream
    StreamId streamId2 = NS1.stream("streamTest2");
    streamProperties = new StreamProperties(1L,
                                            new FormatSpecification("csv", Schema.parseSQL("name string, id int"),
                                                                    ImmutableMap.<String, String>of()), 128, null, BOB);
    streamClient.create(streamId2, streamProperties);
    Assert.assertEquals(streamProperties, streamClient.getConfig(streamId2));


    // Test Datasets
    DatasetClient datasetClient = new DatasetClient(getClientConfig(), getRestClient());
    DatasetId testDatasetInstance = NS1.dataset("testDataset");

    try {
      datasetClient.create(testDatasetInstance, new DatasetInstanceConfiguration("table", null, null, EVE));
      Assert.fail("Expected dataset creation to fail for this user");
    } catch (Exception expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Insufficient permissions")));
      try {
        datasetClient.get(testDatasetInstance);
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getMessage().contains(String.format("was not found")));
      }
    }

    datasetClient.create(testDatasetInstance, new DatasetInstanceConfiguration("table", null, null, ALICE));

    // Verify owner was able to create the dataset
    Assert.assertTrue(datasetClient.exists(testDatasetInstance));

    // Test Apps
    ArtifactId artifactId = NS1.artifact("WikipediaPipelineArtifact", "1.0.0");
    getTestManager().addAppArtifact(artifactId, WikipediaPipelineApp.class);

    ArtifactSummary artifactSummary =  new ArtifactSummary("WikipediaPipelineArtifact", "1.0.0");
    ApplicationId applicationId = NS1.app(WikipediaPipelineApp.class.getSimpleName());

    // deploy should fail when trying with user not in the same group as the namespace config
    try {
      deployApplication(applicationId, new AppRequest(artifactSummary, null, EVE));
      Assert.fail("Expected deploy app to fail for this user");
    } catch (Exception expected) {
      Assert.assertTrue(expected.getMessage().contains(String.format("Insufficient permissions")));
    }

    // Check if application can be deployed by onwer
    deployApplication(applicationId, new AppRequest(artifactSummary, null, ALICE));

    namespaceClient.delete(NS1);
    Assert.assertFalse(namespaceClient.exists(NS1));
  }

  private String getKeytabURIforPrincipal(String principal) throws Exception {
    ConfigEntry configEntry = getMetaClient().getCDAPConfig().get(Constants.Security.KEYTAB_PATH);
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s", Constants.Security.KEYTAB_PATH);
    String name = new KerberosName(principal).getShortName();
    return configEntry.getValue().replace(Constants.USER_NAME_SPECIFIER, name);
  }
}
