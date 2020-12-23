/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.cdap.app.etl.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Tests reading to and writing from Google Cloud Datastore within a Dataproc cluster.
 */
public class GoogleCloudDatastoreTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudDatastoreTest.class);
  private static final String GCD_KIND_1 = "kind_1";
  private static final String GCD_KIND_2 = "kind_2";
  private static final String GCD_PLUGIN_NAME = "Datastore";

  private static String gcdNamespace1;
  private static String gcdNamespace2;
  private static Datastore datastore;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    gcdNamespace1 = "it-ns-1-" + UUID.randomUUID().toString();
    gcdNamespace2 = "it-ns-2-" + UUID.randomUUID().toString();

    GoogleCredentials credentials;
    try (InputStream inputStream = new ByteArrayInputStream(
      getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8))) {
      credentials = GoogleCredentials.fromStream(inputStream);
    }
    datastore = DatastoreOptions.newBuilder()
      .setProjectId(getProjectId())
      .setCredentials(credentials)
      .build()
      .getService();
  }

  @Override
  public void innerSetup() throws Exception {
    // wait for artifact containing Cloud Datastore to load
    Tasks.waitFor(true, () -> {
      try {
        ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!datastorePluginExists(dataPipelineId, BatchSource.PLUGIN_TYPE)) {
          return false;
        }
        return datastorePluginExists(dataPipelineId, BatchSink.PLUGIN_TYPE);
      } catch (ArtifactNotFoundException e) {
        // happens if the relevant artifact(s) were not added yet
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);

    deleteDatasets();
  }

  @Override
  protected void innerTearDown() {
    deleteDatasets();
  }

  @Test
  public void testReadWithFilterAndInsertUsingAutoGeneratedKeyAndCustomIndexes() throws Exception {
    testReadWithFilterAndInsertUsingAutoGeneratedKeyAndCustomIndexes(Engine.MAPREDUCE);
    testReadWithFilterAndInsertUsingAutoGeneratedKeyAndCustomIndexes(Engine.SPARK);
  }

  private void testReadWithFilterAndInsertUsingAutoGeneratedKeyAndCustomIndexes(Engine engine) throws Exception {
    List<String> indexedProperties = Arrays.asList("long_field", "string_field", "double_field", "entity_field",
                                                   "array_field");

    Schema sourceSchema = getInitialSchema();

    // read data from Cloud Datastore using filter id = 1 without key, must read 1 record
    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-input")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${srcKind}")
      .put("filters", "${filters}")
      .put("numSplits", "${numSplits}")
      .put("keyType", "${srcKeyType}")
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-output")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${dstKind}")
      .put("keyType", "${dstKeyType}")
      .put("indexStrategy", "Custom")
      .put("indexedProperties", "${indexedProperties}")
      .put("batchSize", "${batchSize}")
      .build();

    // insert initial dataset to Cloud Datastore
    List<Entity> initialDataset = insertEntities(getInitialDataset(gcdNamespace1, GCD_KIND_1));

    int expectedCount = 1;

    // deploy application and run pipeline
    DeploymentDetails deploymentDetails = deployApplication(sourceProps, sinkProps,
      GCD_PLUGIN_NAME + "-filterAndInsertWithAutoGenKey", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("namespace", gcdNamespace1);
    args.put("srcKind", GCD_KIND_1);
    args.put("dstKind", GCD_KIND_2);
    args.put("filters", "id|1");
    args.put("numSplits", "2");
    args.put("srcKeyType", "None");
    args.put("dstKeyType", "Auto-generated key");
    args.put("indexedProperties", String.join(",", indexedProperties));
    args.put("batchSize", "25");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    // get inserted Data from Cloud Datastore and check that 1 entity is returned
    List<Entity> resultingDataset = getResultingDataset(gcdNamespace1, GCD_KIND_2);
    Assert.assertEquals(expectedCount, resultingDataset.size());

    // find expected entity in the initial dataset
    List<Entity> expectedDataset = initialDataset.stream()
      .filter(i -> i.getLong("id") == 1)
      .collect(Collectors.toList());
    Assert.assertEquals(expectedCount, expectedDataset.size());

    // compare expected and actual entities
    Entity expectedEntity = expectedDataset.get(0);
    Entity actualEntity = resultingDataset.get(0);

    Set<String> names = expectedEntity.getNames();
    // number of fields must be the same
    Assert.assertEquals(names.size(), actualEntity.getNames().size());
    names.forEach(
      name -> {
        Value<?> expectedValue = expectedEntity.getValue(name);
        Value<?> actualValue = actualEntity.getValue(name);
        // field types and values must be the same
        Assert.assertEquals(expectedValue.getType(), actualValue.getType());
        Assert.assertEquals(expectedValue.get(), actualValue.get());
        // check that only selected fields are indexed
        Assert.assertEquals(!indexedProperties.contains(name), actualValue.excludeFromIndexes());
        // check that new records do not have ancestors
        Assert.assertTrue(expectedEntity.getKey().getAncestors().isEmpty());
      });
  }

  @Test
  public void testReadWithFilterAndUpdateUsingUrlSafeKey() throws Exception {
    testReadWithFilterAndUpdateUsingUrlSafeKey(Engine.MAPREDUCE);
    testReadWithFilterAndUpdateUsingUrlSafeKey(Engine.SPARK);
  }

  private void testReadWithFilterAndUpdateUsingUrlSafeKey(Engine engine) throws Exception {
    // prepare schema
    Schema initialSchema = getInitialSchema();
    List<Schema.Field> initialFields = initialSchema.getFields();
    Assert.assertNotNull(initialFields);

    // add only two fields: id and string_field
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(initialSchema.getField("id"));
    fields.add(initialSchema.getField("string_field"));

    // add key field
    fields.add(Schema.Field.of("key", Schema.of(Schema.Type.STRING)));
    Schema sourceSchema = Schema.recordOf("schema", fields);

    // read data from Cloud Datastore using filter id = 2, include Url-safe key, must read 1 record
    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-input")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${srcKind}")
      .put("filters", "${filters}")
      .put("numSplits", "${numSplits}")
      .put("keyType", "${keyType}")
      .put("keyAlias", "${keyAlias}")
      .put("schema", sourceSchema.toString())
      .build();

    // update existing record (remove some fields) in Cloud Datastore using Url-safe key
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-output")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${dstKind}")
      .put("keyType", "${keyType}")
      .put("keyAlias", "${keyAlias}")
      .put("indexStrategy", "All")
      .put("batchSize", "${batchSize}")
      .build();

    // insert initial dataset to Cloud Datastore
    List<Entity> initialDataset = insertEntities(getInitialDataset(gcdNamespace1, GCD_KIND_1));

    // deploy application and run pipeline
    DeploymentDetails deploymentDetails = deployApplication(sourceProps, sinkProps,
      GCD_PLUGIN_NAME + "-filterAndUpdateUsingUrlSafeKey", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("namespace", gcdNamespace1);
    args.put("srcKind", GCD_KIND_1);
    args.put("dstKind", GCD_KIND_1);
    args.put("filters", "id|2");
    args.put("numSplits", "1");
    args.put("keyType", "Url-safe key");
    args.put("keyAlias", "key");
    args.put("batchSize", "25");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    // get Data from Cloud Datastore and check that number of records are same as was initially inserted
    List<Entity> resultingDataset = getResultingDataset(gcdNamespace1, GCD_KIND_1);
    Assert.assertEquals(initialDataset.size(), resultingDataset.size());

    for (Entity entity : resultingDataset) {
      // check that entity with id = 2 has only two fields
      if (entity.getLong("id") == 2) {
        Set<String> names = entity.getNames();
        Assert.assertEquals(2, names.size());
        Assert.assertTrue(names.contains("id"));
        Assert.assertTrue(names.contains("string_field"));
      } else {
        // other entities must have the same number of fields
        Assert.assertEquals(initialSchema.getFields().size(), entity.getNames().size());
      }
    }
  }

  @Test
  public void testReadByAncestorAndInsertUsingKeyLiteral() throws Exception {
    testReadByAncestorAndInsertUsingKeyLiteral(Engine.MAPREDUCE);
    testReadByAncestorAndInsertUsingKeyLiteral(Engine.SPARK);
  }

  private void testReadByAncestorAndInsertUsingKeyLiteral(Engine engine) throws Exception {
    List<PathElement> ancestors = Collections.singletonList(PathElement.of("aKind", "aName"));

    // prepare schema
    Schema initialSchema = getInitialSchema();
    List<Schema.Field> initialFields = initialSchema.getFields();
    Assert.assertNotNull(initialFields);

    // add key field
    List<Schema.Field> fields = new ArrayList<>(initialFields);
    fields.add(Schema.Field.of("key", Schema.of(Schema.Type.STRING)));
    Schema sourceSchema = Schema.recordOf("schema", fields);

    List<FullEntity<IncompleteKey>> sourceDataset = getInitialDataset(gcdNamespace1, GCD_KIND_1);

    // add ancestor to the entity with id = 3, other entities will be without ancestor
    List<FullEntity<IncompleteKey>> updatedDataset = sourceDataset.stream()
      .map(entity -> {
        if (entity.getLong("id") == 3) {
          IncompleteKey key = IncompleteKey.newBuilder(entity.getKey())
            .addAncestors(ancestors)
            .build();
          return FullEntity.newBuilder(entity).setKey(key).build();
        } else {
          return entity;
        }
      }).collect(Collectors.toList());

    // read data from Cloud Datastore with ancestor key(aKind, 'aName'), include Key literal, must read 1 record
    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-input")
      .put("project", "${project}")
      .put("namespace", "${namespace1}")
      .put("kind", "${kind}")
      .put("ancestor", "${ancestor}")
      .put("numSplits", "${numSplits}")
      .put("keyType", "${keyType}")
      .put("keyAlias", "${keyAlias}")
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into different Namespace using Key literal
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-output")
      .put("project", "${project}")
      .put("namespace", "${namespace2}")
      .put("kind", "${kind}")
      .put("keyType", "${keyType}")
      .put("keyAlias", "${keyAlias}")
      .put("indexStrategy", "All")
      .put("batchSize", "${batchSize}")
      .build();

    // insert initial dataset to Cloud Datastore
    insertEntities(updatedDataset);

    int expectedCount = 1;

    // deploy application and run pipeline
    DeploymentDetails deploymentDetails = deployApplication(sourceProps, sinkProps,
      GCD_PLUGIN_NAME + "-ancestorFilterAndKeyLiteralInsert", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("namespace1", gcdNamespace1);
    args.put("namespace2", gcdNamespace2);
    args.put("kind", GCD_KIND_1);
    args.put("ancestor", "key(aKind, 'aName')");
    args.put("numSplits", "1");
    args.put("keyType", "Key literal");
    args.put("keyAlias", "key");
    args.put("batchSize", "25");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    // get Data from Cloud Datastore and check that only 1 entity is returned
    List<Entity> resultingDataset = getResultingDataset(gcdNamespace2, GCD_KIND_1);
    Assert.assertEquals(expectedCount, resultingDataset.size());

    // check that returned entity has expected ancestors
    Entity entity = resultingDataset.get(0);
    Key key = entity.getKey();
    Assert.assertEquals(ancestors, key.getAncestors());
  }

  @Test
  public void testInsertWithCustomKeyAndAncestor() throws Exception {
    testInsertWithCustomKeyAndAncestor(Engine.MAPREDUCE);
    testInsertWithCustomKeyAndAncestor(Engine.SPARK);
  }

  private void testInsertWithCustomKeyAndAncestor(Engine engine) throws Exception {
    List<PathElement> ancestors = Collections.singletonList(PathElement.of("aKind", "aName"));

    // prepare schema
    Schema sourceSchema = getInitialSchema();
    List<Schema.Field> initialFields = sourceSchema.getFields();
    Assert.assertNotNull(initialFields);

    // read data from Cloud Datastore with any filters, not including key
    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-input")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${srcKind}")
      .put("numSplits", "${numSplits}")
      .put("keyType", "None")
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into new Kind within the same Namespace using custom key with ancestor
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "datastore-output")
      .put("project", "${project}")
      .put("namespace", "${namespace}")
      .put("kind", "${dstKind}")
      .put("keyType", "${keyType}")
      .put("keyAlias", "${keyAlias}")
      .put("ancestor", "${ancestor}")
      .put("indexStrategy", "All")
      .put("batchSize", "${batchSize}")
      .build();

    // insert initial dataset to Cloud Datastore
    List<Entity> initialDataset = insertEntities(getInitialDataset(gcdNamespace1, GCD_KIND_1));

    // deploy application and run pipeline
    DeploymentDetails deploymentDetails = deployApplication(sourceProps, sinkProps,
      GCD_PLUGIN_NAME + "-customKeyAndAncestor", engine);
    Map<String, String> args = new HashMap<>();
    args.put("project", getProjectId());
    args.put("namespace", gcdNamespace1);
    args.put("srcKind", GCD_KIND_1);
    args.put("dstKind", GCD_KIND_2);
    args.put("numSplits", "2");
    args.put("keyType", "Custom name");
    args.put("keyAlias", "string_field");
    args.put("ancestor", "key(aKind, 'aName')");
    args.put("batchSize", "25");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED, args);

    // get Data from Cloud Datastore and check that number of records are same as was initially inserted
    List<Entity> resultingDataset = getResultingDataset(gcdNamespace1, GCD_KIND_2);
    Assert.assertEquals(initialDataset.size(), resultingDataset.size());

    // since one field was used as custom key, resulting number of fields must be less by 1 from initial
    int expectedNumberOfFields = initialFields.size() - 1;

    resultingDataset.forEach(
      entity -> {
        Set<String> names = entity.getNames();
        Assert.assertEquals(expectedNumberOfFields, names.size());
        // check that fields used as custom key is absent
        Assert.assertFalse(names.contains("string_field"));
        Key key = entity.getKey();
        // check that key is named
        Assert.assertTrue(key.hasName());
        // check that entity has expected ancestors
        Assert.assertEquals(ancestors, key.getAncestors());
      });
  }

  private boolean datastorePluginExists(ArtifactId dataPipelineId, String pluginType) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM).stream()
      .anyMatch(pluginSummary -> GCD_PLUGIN_NAME.equals(pluginSummary.getName()));
  }

  private DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                              Map<String, String> sinkProperties,
                                              String applicationName, Engine engine) throws Exception {
    ETLStage source = new ETLStage("DatastoreSourceStage",
                                   new ETLPlugin(GCD_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 sourceProperties,
                                                 GOOGLE_CLOUD_ARTIFACT));
    ETLStage sink = new ETLStage("DatastoreSinkStage", new ETLPlugin(GCD_PLUGIN_NAME,
                                                                     BatchSink.PLUGIN_TYPE,
                                                                     sinkProperties,
                                                                     GOOGLE_CLOUD_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private List<Entity> insertEntities(List<FullEntity<IncompleteKey>> entities) {
    return datastore.add(entities.toArray(new FullEntity[0]));
  }

  private List<Entity> getResultingDataset(String namespace, String kind) {
    Query<Entity> query = Query.newEntityQueryBuilder()
      .setNamespace(namespace)
      .setKind(kind)
      .build();
    QueryResults<Entity> results = datastore.run(query);
    Iterable<Entity> iterable = () -> results;

    return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
  }

  private void deleteDatasets() {
    for (String namespace : Arrays.asList(gcdNamespace1, gcdNamespace2)) {
      for (String kind : Arrays.asList(GCD_KIND_1, GCD_KIND_2)) {
        try {
          KeyQuery keyQuery = Query.newKeyQueryBuilder().setNamespace(namespace).setKind(kind).build();
          QueryResults<Key> keys = datastore.run(keyQuery);
          Iterable<Key> iterable = () -> keys;
          Key[] keysToDelete = StreamSupport.stream(iterable.spliterator(), false).toArray(Key[]::new);
          if (keysToDelete.length > 0) {
            LOG.info("Deleting all Cloud Datastore entities from namespace '{}',  kind '{}'", namespace, kind);
            datastore.delete(keysToDelete);
          }
        } catch (DatastoreException e) {
          LOG.error("Unable to delete Cloud datastore kind entities from namespace '{}',  kind '{}'",
            namespace, kind, e);
        }
      }
    }
  }

  private Schema getInitialSchema() {
    return Schema.recordOf("datastoreSourceSchema",
             Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
             Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
             Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
             Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
             Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
             Schema.Field.of("timestamp_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
             Schema.Field.of("blob_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
             Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("entity_field",
               Schema.nullableOf(Schema.recordOf("entity_field",
                 Schema.Field.of("nested_string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                 Schema.Field.of("nested_long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
             Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
             Schema.Field.of("union_field", Schema.unionOf(
                Schema.of(Schema.Type.STRING),
                Schema.of(Schema.Type.LONG),
                Schema.of(Schema.Type.BOOLEAN)
              ))
    );
  }

  private List<FullEntity<IncompleteKey>> getInitialDataset(String namespace, String kind) {
    IncompleteKey key = new KeyFactory(getProjectId())
      .setNamespace(namespace)
      .setKind(kind)
      .newKey();
    return Arrays.asList(
      getFullEntity(key, 1, "string_value-1", 111111111111111L, 111111.11111111111, true,
                    "2011-01-01T01:01:01Z", "blob_value_1", "nested_string_value-1", 11L,
                    Collections.emptyList(), StringValue.of("union_value_1")),
      getFullEntity(key, 2, "string_value-2", 222222222222222L, 22222.22222222222, false,
                    "2012-02-02T02:02:02Z", "blob_value_2", "nested_string_value-2", 22L,
                    Arrays.asList("a", null), LongValue.of(2)),
      getFullEntity(key, 3, "string_value-3", 333333333333L, 3333.3333333333, true,
                    "2013-03-03T03:03:03Z", "blob_value_3", "nested_string_value-3", 33L,
                    Arrays.asList("a", "b"), BooleanValue.of(false)),
      getFullEntity(key, 4, "string_value-4", 4444444444444L, 4444.4444444444, false,
                    "2014-04-04T04:04:04Z", "blob_value_4", "nested_string_value-4", 44L,
                    Arrays.asList("c", "d", "e"), StringValue.of("union_value_4")),
      getFullEntity(key, 5, "string_value-5", 555555555555L, 5555.5555555555, true,
                    "2015-05-05T05:05:05Z", "blob_value_5", "nested_string_value-5", 55L,
                    Arrays.asList(null, "f", "g"), LongValue.of(5)),
      getFullEntity(key, 6, "string_value-6", 6666666666L, 666.6666666666, false,
                    "2016-06-06T06:06:06Z", "blob_value_6", "nested_string_value-6", 66L,
                    Arrays.asList("h", "i", "j"), BooleanValue.of(true)),
      getFullEntity(key, 7, "string_value-7", 777777777777L, 7777.7777777777, true,
                    "2017-07-07T07:07:07Z", "blob_value_7", "nested_string_value-7", 77L,
                    Collections.emptyList(), StringValue.of("union_value_7")),
      getFullEntity(key, 8, "string_value-8", 88888888888L, 8888.8888888888, false,
                    "2018-08-08T08:08:08Z", "blob_value_8", "nested_string_value-8", 88L,
                    Arrays.asList("k", null, "l"), LongValue.of(8)),
      getFullEntity(key, 9, "string_value-9", 9999999999L, 9999.9999999999, true,
                    "2019-09-09T09:09:09Z", "blob_value_9", "nested_string_value-9", 99L,
                    Collections.emptyList(), BooleanValue.of(false)),
      getFullEntity(key, 10, "string_value-10", 1010101010101010000L, 10101010.1010101, false,
                    "2020-10-10T10:10:10Z", "blob_value_10", "nested_string_value-10", 100L,
                    Arrays.asList("n", "o", "p"), StringValue.of("union_value_10"))
      );
  }

  private FullEntity<IncompleteKey> getFullEntity(IncompleteKey key,
                                                  long id,
                                                  String stringValue,
                                                  Long longValue,
                                                  Double doubleValue,
                                                  Boolean booleanValue,
                                                  String timestamp,
                                                  String blob,
                                                  String nestedString,
                                                  Long nestedLong,
                                                  List<String> stringList,
                                                  Value<?> unionValue) {
    return FullEntity.newBuilder()
          .setKey(key)
          .set("id", id)
          .set("string_field", stringValue)
          .set("long_field", longValue)
          .set("double_field", doubleValue)
          .set("boolean_field", booleanValue)
          .set("timestamp_field", Timestamp.parseTimestamp(timestamp))
          .set("blob_field", Blob.copyFrom(blob.getBytes()))
          .setNull("null_field")
          .set("entity_field", Entity.newBuilder()
            .set("nested_string_field", nestedString)
            .set("nested_long_field", nestedLong)
            .build())
          .set("array_field", stringList.stream()
                                        .map(s -> s == null ? NullValue.of() : StringValue.of(s))
                                        .collect(Collectors.toList()))
          .set("union_field", unionValue)
          .build();
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }
}
