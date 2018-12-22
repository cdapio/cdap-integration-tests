/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.client.LineageClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.MetadataConstants;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test which uses {@link ProgramMetadataApp} to tests reading and emitting metadata from programs. Also test lineage
 * functionality.
 */
public class ProgramMetadataTest extends AudiTestBase {

  private static final ApplicationId APP = TEST_NAMESPACE.app(ProgramMetadataApp.APP_NAME);
  private static final ProgramId PROGRAM = APP.program(ProgramType.MAPREDUCE, ProgramMetadataApp.MetadataMR.NAME);
  private static final DatasetId INPUT_DATASET = TEST_NAMESPACE.dataset(ProgramMetadataApp.INPUT_DATASET_NAME);
  private static final DatasetId OUTPUT_DATASET = TEST_NAMESPACE.dataset(ProgramMetadataApp.OUTPUT_DATASET_NAME);

  private MetadataClient metadataClient;
  private LineageClient lineageClient;

  @Before
  public void setup() {
    metadataClient = new MetadataClient(getClientConfig(), getRestClient());
    lineageClient = new LineageClient(getClientConfig(), getRestClient());
  }

  @Test
  public void testWithLineage() throws Exception {
    ProgramClient programClient = getProgramClient();
    ApplicationManager applicationManager = deployApplication(ProgramMetadataApp.class);

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long endTime = startTime + 10000;
    // assert no lineage for datasets yet
    Assert.assertEquals(LineageSerializer.toLineageRecord(startTime, endTime, new Lineage(ImmutableSet.of()),
        ImmutableSet.of()),
        lineageClient.getLineage(INPUT_DATASET, startTime, endTime, null));
    Assert.assertEquals(LineageSerializer.toLineageRecord(startTime, endTime, new Lineage(ImmutableSet.of()),
        ImmutableSet.of()),
        lineageClient.getLineage(OUTPUT_DATASET, startTime, endTime, null));

    MapReduceManager mapReduceManager =
      applicationManager.getMapReduceManager(PROGRAM.getProgram());
    mapReduceManager.start();
    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<RunRecord> runRecords = getRunRecords(1, programClient, PROGRAM,
                                               ProgramRunStatus.COMPLETED.name(), 0, endTime);
    LineageRecord expected =
      LineageSerializer.toLineageRecord(
        startTime, endTime,
        new Lineage(ImmutableSet.of(new Relation(INPUT_DATASET, PROGRAM, AccessType.READ,
                                                 RunIds.fromString(runRecords.get(0).getPid())),
                                    new Relation(OUTPUT_DATASET, PROGRAM, AccessType.WRITE,
                                                 RunIds.fromString(runRecords.get(0).getPid())))), ImmutableSet.of());
    waitFor(expected, () -> lineageClient.getLineage(INPUT_DATASET, startTime, endTime, null));
    waitFor(expected, () -> lineageClient.getLineage(OUTPUT_DATASET, startTime, endTime, null));
  }

  @Test
  public void testSearchUsingSystemMetadata() throws Exception {
    deployApplication(ProgramMetadataApp.class);
    assertArtifactSearch();
    assertAppSearch();
    assertProgramSearch();
    assertDatasetSearch();
  }

  private <T> void waitFor(T expected, Callable<T> callable) throws Exception {
    Tasks.waitFor(expected, callable, 60, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }

  private void assertArtifactSearch() throws Exception {
    String artifactName = "system-metadata-artifact";
    String pluginArtifactName = "system-metadata-plugins";
    ArtifactId systemMetadataArtifact = TEST_NAMESPACE.artifact(artifactName, "1.0.0");
    getTestManager().addAppArtifact(systemMetadataArtifact, ArtifactSystemMetadataApp.class);
    ArtifactId pluginArtifact = TEST_NAMESPACE.artifact(pluginArtifactName, "1.0.0");
    getTestManager().addPluginArtifact(pluginArtifact, systemMetadataArtifact,
                                       ArtifactSystemMetadataApp.EchoPlugin1.class,
                                       ArtifactSystemMetadataApp.EchoPlugin2.class);

    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(systemMetadataArtifact, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(pluginArtifact, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    // verify search using artifact name
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemMetadataArtifact)),
      searchMetadata(TEST_NAMESPACE, artifactName, null)
    );
    // verify search using plugin name
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(pluginArtifact)),
      searchMetadata(TEST_NAMESPACE,
                     ArtifactSystemMetadataApp.PLUGIN1_NAME, EntityTypeSimpleName.ARTIFACT)
    );
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(pluginArtifact)),
      searchMetadata(TEST_NAMESPACE,
                     ArtifactSystemMetadataApp.PLUGIN2_NAME, null)
    );
  }

  private void assertAppSearch() throws Exception {
    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(APP, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(PROGRAM, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    // using app name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, APP.getEntityName(), null));
    // using program names
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PROGRAM.getEntityName(),
                                                 EntityTypeSimpleName.APP));
    // using program types
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.MAPREDUCE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                     EntityTypeSimpleName.APP));
  }

  private void assertProgramSearch() throws Exception {
    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(PROGRAM, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, "batch", EntityTypeSimpleName.PROGRAM));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, PROGRAM.getEntityName(), EntityTypeSimpleName.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, ProgramType.MAPREDUCE.getPrettyName(),
                     EntityTypeSimpleName.PROGRAM));
  }

  private void assertDatasetSearch() throws Exception {
    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(INPUT_DATASET, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(OUTPUT_DATASET, MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    metadataClient.addProperties(INPUT_DATASET, datasetProperties);

    Set<MetadataSearchResultRecord> result = searchMetadata(TEST_NAMESPACE, "schema:*", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(INPUT_DATASET))
                          .build(),
                        result);

    // search dataset
    Set<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(INPUT_DATASET), new MetadataSearchResultRecord(OUTPUT_DATASET)
    );
    result = searchMetadata(TEST_NAMESPACE, "batch", EntityTypeSimpleName.DATASET);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "explore", EntityTypeSimpleName.DATASET);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, KeyValueTable.class.getName(), null);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "type:*", null);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "dataset*", EntityTypeSimpleName.DATASET);
    Assert.assertEquals(expectedKvTables, result);

    result = searchMetadata(TEST_NAMESPACE, INPUT_DATASET.getEntityName(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(INPUT_DATASET)), result
    );
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespace, String query,
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
