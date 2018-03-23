/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests system metadata for ETL apps.
 */
public class ETLSystemMetadataTest extends ETLTestBase {

  @Test
  public void testSearchETLArtifactsWithSystemMetadata() throws Exception {
    MetadataClient metadataClient = new MetadataClient(getClientConfig(), getRestClient());
    String version = getMetaClient().getVersion().getVersion();
    ArtifactId batchId = NamespaceId.SYSTEM.artifact("cdap-data-pipeline", version);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(batchId));
    Set<MetadataSearchResultRecord> result =
      searchMetadata(metadataClient, NamespaceId.SYSTEM, "cdap-data-pipeline", null);
    Assert.assertEquals(expected, result);
    result = searchMetadata(metadataClient, NamespaceId.SYSTEM, "cdap-data-p*", EntityTypeSimpleName.ARTIFACT);
    Assert.assertEquals(expected, result);
    ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());
    List<ArtifactSummary> allCorePlugins = artifactClient.listVersions(TEST_NAMESPACE, "core-plugins",
                                                                       ArtifactScope.SYSTEM);
    Assert.assertTrue("Expected at least one core-plugins artifact.", allCorePlugins.size() > 0);
    String corePluginsVersion = allCorePlugins.get(0).getVersion();
    ArtifactId corePlugins = NamespaceId.SYSTEM.artifact("core-plugins", corePluginsVersion);
    expected = ImmutableSet.of(new MetadataSearchResultRecord(corePlugins));
    result = searchMetadata(metadataClient, NamespaceId.SYSTEM, "table", EntityTypeSimpleName.ARTIFACT);
    Assert.assertEquals(expected, result);
    // Searching in some user namespace should also surface entities from the system namespace
    expected = ImmutableSet.of(new MetadataSearchResultRecord(
      NamespaceId.SYSTEM.artifact("cdap-data-pipeline", getMetaClient().getVersion().getVersion())));
    result = searchMetadata(metadataClient, TEST_NAMESPACE, "cdap-data-pipeline", null);
    Assert.assertEquals(expected, result);
  }
  
  private Set<MetadataSearchResultRecord> searchMetadata(MetadataClient metadataClient,
                                                         NamespaceId namespace, String query,
                                                         EntityTypeSimpleName targetType) throws Exception {
    Set<MetadataSearchResultRecord> results =
      metadataClient.searchMetadata(namespace, query, targetType).getResults();
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
