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
package io.cdap.cdap.apps.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.client.LineageClient;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageSerializer;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.MapReduceManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

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

  // TODO: (CDAP-20464) re-enable after logging deadlock is fixed
  @Ignore
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
    startAndWaitForRun(mapReduceManager, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
                  () -> metadataClient.getProperties(systemMetadataArtifact.toMetadataEntity(),
                  MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(pluginArtifact.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
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
                     ArtifactSystemMetadataApp.PLUGIN1_NAME, MetadataEntity.ARTIFACT)
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
                  () -> metadataClient.getProperties(APP.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(PROGRAM.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    // using app name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(APP));
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, APP.getEntityName(), null));
    // using program names
    Assert.assertEquals(expected, searchMetadata(TEST_NAMESPACE, PROGRAM.getEntityName(),
                                                 MetadataEntity.APPLICATION));
    // using program types
    Assert.assertEquals(
      expected,
      searchMetadata(TEST_NAMESPACE,
                     ProgramType.MAPREDUCE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                     MetadataEntity.APPLICATION));
  }

  private void assertProgramSearch() throws Exception {
    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(PROGRAM.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, "batch", MetadataEntity.PROGRAM));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, PROGRAM.getEntityName(), MetadataEntity.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PROGRAM)),
      searchMetadata(TEST_NAMESPACE, ProgramType.MAPREDUCE.getPrettyName(),
                     MetadataEntity.PROGRAM));
  }

  private void assertDatasetSearch() throws Exception {
    // Wait for system metadata to be populated
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(INPUT_DATASET.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(false,
                  () -> metadataClient.getProperties(OUTPUT_DATASET.toMetadataEntity(), MetadataScope.SYSTEM).isEmpty(),
                  10, TimeUnit.SECONDS);

    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    metadataClient.addProperties(INPUT_DATASET.toMetadataEntity(), datasetProperties);

    Set<MetadataSearchResultRecord> result = searchMetadata(TEST_NAMESPACE, "schema:*", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(INPUT_DATASET))
                          .build(),
                        result);

    // search dataset
    Set<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(INPUT_DATASET), new MetadataSearchResultRecord(OUTPUT_DATASET)
    );
    result = searchMetadata(TEST_NAMESPACE, "batch", MetadataEntity.DATASET);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, KeyValueTable.class.getName(), null);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "type:*", null);
    Assert.assertEquals(expectedKvTables, result);
    result = searchMetadata(TEST_NAMESPACE, "dataset*", MetadataEntity.DATASET);
    Assert.assertEquals(expectedKvTables, result);

    result = searchMetadata(TEST_NAMESPACE, INPUT_DATASET.getEntityName(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(INPUT_DATASET)), result
    );
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespace, String query,
                                                         @Nullable String targetType) throws Exception {
    Set<MetadataSearchResultRecord> results =
      metadataClient.searchMetadata(namespace, query, targetType).getResults();
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
