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
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
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
    Id.Artifact batchId = Id.Artifact.from(Id.Namespace.SYSTEM, "cdap-etl-batch", version);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(batchId));
    Set<MetadataSearchResultRecord> result =
      searchMetadata(metadataClient, Id.Namespace.SYSTEM, "cdap-etl-batch", null);
    Assert.assertEquals(expected, result);
    result = searchMetadata(metadataClient, Id.Namespace.SYSTEM, "cdap-etl-b*", MetadataSearchTargetType.ARTIFACT);
    Assert.assertEquals(expected, result);
    ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());
    List<ArtifactSummary> allCorePlugins = artifactClient.listVersions(Id.Namespace.DEFAULT, "core-plugins",
                                                                       ArtifactScope.SYSTEM);
    Assert.assertTrue("Expected at least one core-plugins artifact.", allCorePlugins.size() > 0);
    String corePluginsVersion = allCorePlugins.get(0).getVersion();
    Id.Artifact corePlugins = Id.Artifact.from(Id.Namespace.SYSTEM, "core-plugins", corePluginsVersion);
    expected = ImmutableSet.of(new MetadataSearchResultRecord(corePlugins));
    result = searchMetadata(metadataClient, Id.Namespace.SYSTEM, "table", MetadataSearchTargetType.ARTIFACT);
    Assert.assertEquals(expected, result);
    // Searching in the default namespace should also surface entities from the system namespace
    expected = ImmutableSet.of(new MetadataSearchResultRecord(
      Id.Artifact.from(Id.Namespace.SYSTEM, "cdap-etl-batch", getMetaClient().getVersion().getVersion())));
    result = searchMetadata(metadataClient, Id.Namespace.DEFAULT, "batch", null);
    Assert.assertEquals(expected, result);
    expected = ImmutableSet.of(new MetadataSearchResultRecord(
      Id.Artifact.from(Id.Namespace.SYSTEM, "cdap-etl-realtime", getMetaClient().getVersion().getVersion())));
    result = searchMetadata(metadataClient, Id.Namespace.DEFAULT, "realtime", null);
    Assert.assertEquals(expected, result);
  }
  
  private Set<MetadataSearchResultRecord> searchMetadata(MetadataClient metadataClient,
                                                         Id.Namespace namespace, String query,
                                                         MetadataSearchTargetType targetType) throws Exception {
    Set<MetadataSearchResultRecord> results = metadataClient.searchMetadata(namespace, query, targetType);
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
