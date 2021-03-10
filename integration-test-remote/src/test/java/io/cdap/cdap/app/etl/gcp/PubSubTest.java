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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.suite.category.RequiresSpark;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.directory.api.util.Strings;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Follow up CDAP-17648 pubSubToPubSubCSVSink currently doesn't work because of the SINK which will write a memory
 * address instead of the actual content when the type is byte in CSV Track https://cdap.atlassian.net/browse/CDAP-17648
 * for more details
 */
@Ignore
public class PubSubTest extends DataprocETLTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubTest.class);
  private static final String GOOGLE_SUBSCRIBER_PLUGIN_NAME = "GoogleSubscriber";
  private static final String GOOGLE_PUBLISHER_PLUGIN_NAME = "GooglePublisher";
  private static final String TEXT = "text";
  private static final String BLOB = "blob";
  private static final String SCHEMA = "schema";
  private static final String DELIMITER = "delimiter";
  private static final String FORMAT = "format";
  private static final String TOPIC = "topic";
  private static final String REFERENCE_NAME = "referenceName";
  private static final String SUBSCRIPTION = "subscription";
  private static final String PROJECT = "project";
  private static final String JSON = "json";
  private static final String CSV = "csv";
  private static final String TSV = "tsv";
  private static final String DELIMITED = "delimited";
  private static final String AVRO = "avro";
  private static final String PARQUET = "parquet";

  private static TopicAdminClient topicAdmin;
  private static SubscriptionAdminClient subscriptionAdmin;
  private ArrayList<ProjectTopicName> topicsToDelete;
  private ArrayList<ProjectSubscriptionName> subscriptionsToDelete;
  private static final Schema MESSAGE_SCHEMA = Schema.recordOf(
    "message",
    Schema.Field.of("message", Schema.of(Schema.Type.STRING)));

  private static final Schema RECORD_SCHEMA =
    Schema.recordOf(SCHEMA,
                    Schema.Field.of("message", MESSAGE_SCHEMA),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf(SCHEMA,
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );

  @BeforeClass
  public static void testClassSetup() throws IOException {
    topicAdmin = TopicAdminClient.create(
      TopicAdminSettings.newBuilder().setCredentialsProvider(getCredentialProvider()).build());
    subscriptionAdmin = SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder().setCredentialsProvider(getCredentialProvider()).build());
  }

  @AfterClass
  public static void testClassTeardown() {
    // Shutdown topic admin
    try {
      topicAdmin.shutdownNow();
      topicAdmin.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.warn("Failed to shutdown PubSub topic admin: {}", e.getMessage());
    }

    // Shutdown subscription admin
    try {
      subscriptionAdmin.shutdownNow();
      subscriptionAdmin.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.warn("Failed to shutdown PubSub subscription admin: {}", e.getMessage());
    }
  }

  private static CredentialsProvider getCredentialProvider() {
    return () -> GoogleCredentials.fromStream(
      new ByteArrayInputStream(getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8)));
  }

  void checkPluginExists(String pluginName, String pluginType, String artifact) {
    Preconditions.checkNotNull(pluginName);
    Preconditions.checkNotNull(pluginType);
    Preconditions.checkNotNull(artifact);

    try {
      Tasks.waitFor(true, () -> {
        try {
          final ArtifactId artifactId = TEST_NAMESPACE.artifact(artifact, version);
          List<PluginSummary> plugins =
            artifactClient.getPluginSummaries(artifactId, pluginType, ArtifactScope.SYSTEM);
          return plugins.stream().anyMatch(pluginSummary -> pluginName.equals(pluginSummary.getName()));
        } catch (ArtifactNotFoundException e) {
          // happens if the relevant artifact(s) were not added yet
          return false;
        }
      }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void innerSetup() {
    ImmutableList.of(ImmutableList.of(GOOGLE_SUBSCRIBER_PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, "cdap-data-streams"),
                     ImmutableList.of(GOOGLE_PUBLISHER_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, "cdap-data-pipeline"))
      .forEach((pluginInfo) -> checkPluginExists(pluginInfo.get(0), pluginInfo.get(1), pluginInfo.get(2)));

    topicsToDelete = new ArrayList<>();
    subscriptionsToDelete = new ArrayList<>();
  }

  @Override
  public void innerTearDown() {
    for (ProjectTopicName topic : topicsToDelete) {
      try {
        topicAdmin.deleteTopic(topic);
        LOG.info("Deleted topic {}", topic.toString());
      } catch (ApiException ex) {
        LOG.error(String.format("Failed to delete topic %s", topic.toString()), ex);
      }
    }
    for (ProjectSubscriptionName subscription : subscriptionsToDelete) {
      try {
        subscriptionAdmin.deleteSubscription(subscription);
        LOG.info("Deleted subscription {}", subscription.toString());
      } catch (ApiException ex) {
        LOG.error(String.format("Failed to delete subscription %s", subscription.toString()), ex);
      }
    }
  }

  private ProjectTopicName createTopic(String topicName) {
    ProjectTopicName result = ProjectTopicName.of(getProjectId(), topicName);
    topicAdmin.createTopic(result);
    topicsToDelete.add(result);
    LOG.info("Created topic {}", result.toString());
    return result;
  }

  private ProjectSubscriptionName createSubscription(String topicName, String subscriptionName) {
    ProjectSubscriptionName result = ProjectSubscriptionName.of(getProjectId(), subscriptionName);
    subscriptionAdmin.createSubscription(result, ProjectTopicName.of(getProjectId(), topicName),
                                         PushConfig.getDefaultInstance(), 0);
    subscriptionsToDelete.add(result);
    LOG.info("Created subscription {}", result.toString());
    return result;
  }

  private void markSubscriptionForCleanup(String subscriptionName) {
    ProjectSubscriptionName result = ProjectSubscriptionName.of(getProjectId(), subscriptionName);
    subscriptionsToDelete.add(result);
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubTest() throws Exception {
    String prefix = "cdap-msg";

    String sourceTopicName = String.format("%s-source-%s", prefix, UUID.randomUUID());
    String sinkTopicName = String.format("%s-sink-%s", prefix, UUID.randomUUID());

    String testSubscriptionName = String.format("%s-test-%s", prefix, UUID.randomUUID());
    String pipelineReadSubscriptionName = String.format("%s-pipeline-%s", prefix, UUID.randomUUID());
    markSubscriptionForCleanup(pipelineReadSubscriptionName);

    ProjectTopicName sourceTopicNamePTN = createTopic(sourceTopicName);
    createTopic(sinkTopicName);

    ProjectSubscriptionName testSubscriptionPSN = createSubscription(sinkTopicName, testSubscriptionName);

    TestMessageReceiver receiver = new TestMessageReceiver();
    Subscriber subscriber = Subscriber.newBuilder(testSubscriptionPSN, receiver)
      .setCredentialsProvider(getCredentialProvider()).build();

    subscriber.addListener(new Subscriber.Listener() {
      public void failed(Subscriber.State from, Throwable failure) {
        LOG.error("State {}:", from.toString(), failure);
      }
    }, MoreExecutors.directExecutor());

    subscriber.startAsync().awaitRunning();

    ETLStage ss = createSubscriberStage(sourceTopicName, pipelineReadSubscriptionName);
    ETLStage ps = createPublisherStage(sinkTopicName);

    DataStreamsConfig config = DataStreamsConfig.builder()
      .addStage(ss)
      .addStage(ps)
      .addConnection(ss.getName(), ps.getName())
      .setBatchInterval("30s")
      .setStopGracefully(false)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("PubSubTest");
    AppRequest<DataStreamsConfig> appRequest = getStreamingAppRequest(config);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    try {
      // it takes time to spin-up dataproc cluster, lets wait a little bit
      startAndWaitForRun(sparkManager, ProgramRunStatus.RUNNING,
                         Collections.singletonMap("system.profile.name", getProfileName()), 10, TimeUnit.MINUTES);

      // wait and check for source subscription to be created
      ensureSubscriptionCreated(pipelineReadSubscriptionName, 10, TimeUnit.MINUTES);

      // send some messages...
      publishMessages(sourceTopicNamePTN, "message1", "message3", "message2");
      // ... and ensure they are passed through pipeline to our subscriber
      receiver.assertRetrievedMessagesContain(10, TimeUnit.MINUTES, "message1", "message3", "message2");

      subscriber.stopAsync().awaitTerminated();
    } finally {
      try {
        sparkManager.stop();
        sparkManager.waitForStopped(5, TimeUnit.MINUTES);
      } catch (Exception e) {
        // don't treat this as a test failure, but log a warning
        LOG.warn("Pipeline failed to stop gracefully", e);
      }
    }
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubJson() throws Exception {
    String message = "{\"message\":\"Hello\"}";
    pubSubToPubSubTestFormats(JSON, JSON, null, ByteString.copyFromUtf8(message), message,
                              new JsonPublisherMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubAvro() throws Exception {
    final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
      parse(String.valueOf(MESSAGE_SCHEMA));

    final GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("message", "hello");

    ByteString data = genericRecordToByteString(avroSchema, record);
    pubSubToPubSubTestFormats(AVRO, AVRO, null, data, data, new AvroAndParquetPublisherReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubParquet() throws Exception {
    final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
      parse(String.valueOf(MESSAGE_SCHEMA));

    final GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("message", "This is a simple test message");

    ByteString data = genericRecordToByteString(avroSchema, record);
    pubSubToPubSubTestFormats(PARQUET, PARQUET, null, data, data, new AvroAndParquetPublisherReceiver());
  }

  // Can not test CSV to CSV because:
  // receivedMessage will be of type csv in source and it will return a StructuredRecord{}
  // This will result in multi-level structure data in Sink and it can't be transformed to csv
  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubCSVSource() throws Exception {
    String message = "Hello";
    pubSubToPubSubTestFormats(CSV, JSON, null, ByteString.copyFromUtf8(message), message,
                              new DelimitedSourceToJsonPublisherMessageReceiver());
  }

  // Can not test TSV to TSV because:
  // receivedMessage will be of type csv in source and it will return a StructuredRecord{}
  // This will result in multi-level structure data in Sink and it can't be transformed to tsv
  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubTSVSource() throws Exception {
    String message = "Hello";
    pubSubToPubSubTestFormats(TSV, JSON, "\t", ByteString.copyFromUtf8(message), message,
                              new DelimitedSourceToJsonPublisherMessageReceiver());
  }

  // Can not test DELIMITED to DELIMITED because:
  // receivedMessage will be of type csv in source and it will return a StructuredRecord{}
  // This will result in multi-level structure data in Sink and it can't be transformed to delimited
  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubDelimitedSource() throws Exception {
    String message = "Hello";
    pubSubToPubSubTestFormats(DELIMITED, JSON, ";", ByteString.copyFromUtf8(message), message,
                              new DelimitedSourceToJsonPublisherMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubBlobSource() throws Exception {
    String message = "This is a simple test in blob format";
    pubSubToPubSubTestFormats(BLOB, JSON, null, ByteString.copyFromUtf8(message), message,
                              new TestMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubTextSource() throws Exception {
    String message = "This is a simple test in txt format";
    pubSubToPubSubTestFormats(TEXT, JSON, null, ByteString.copyFromUtf8(message), message,
                              new TestMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubTextSink() throws Exception {
    String message = "This is a simple test in txt format";
    final ByteString bytesMessage = ByteString.copyFromUtf8(message);
    pubSubToPubSubTestFormats(TEXT, TEXT, null, bytesMessage, message, new TestMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubBlobSink() throws Exception {
    String message = "This is a simple test in blob format";
    final ByteString bytesMessage = ByteString.copyFromUtf8(message);
    pubSubToPubSubTestFormats(BLOB, BLOB, null, bytesMessage, message, new TestMessageReceiver());
  }

  //TODO: (CDAP-17648) fix and un-ignore
  @Ignore
  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubCSVSink() throws Exception {
    String message = "Hello";
    pubSubToPubSubTestFormats(TEXT, CSV, null, ByteString.copyFromUtf8(message), message,
                              new DelimitedPublisherMessageReceiver(","));
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubAvroToText() throws Exception {
    String message = "{\"message\":\"hello\"}";

    final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
      parse(String.valueOf(MESSAGE_SCHEMA));

    final GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("message", "hello");

    ByteString data = genericRecordToByteString(avroSchema, record);
    pubSubToPubSubTestFormats(AVRO, TEXT, null, data, message, new JsonPublisherMessageReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubJsonToAvro() throws Exception {
    String message = "{\"message\":\"hello\"}";

    final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
      parse(String.valueOf(MESSAGE_SCHEMA));
    final GenericData.Record record = new GenericData.Record(avroSchema);

    record.put("message", "hello");
    ByteString data = genericRecordToByteString(avroSchema, record);

    pubSubToPubSubTestFormats(JSON, AVRO, null, ByteString.copyFromUtf8(message), data,
                              new AvroAndParquetPublisherReceiver());
  }

  @Category({RequiresSpark.class})
  @Test
  public void pubSubToPubSubJsonToParquet() throws Exception {
    String message = "{\"message\":\"hello\"}";
    final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
      parse(String.valueOf(MESSAGE_SCHEMA));
    final GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("message", "hello");
    ByteString data = genericRecordToByteString(avroSchema, record);
    pubSubToPubSubTestFormats(JSON, PARQUET, null, ByteString.copyFromUtf8(message), data,
                              new AvroAndParquetPublisherReceiver());
  }

  private void pubSubToPubSubTestFormats(String sourceFormat, String sinkFormat, String delimiter, ByteString messages,
                                         Object expectedMessage, PublisherMessageReceiver receiver) throws Exception {
    String prefix = "cdap-msg";

    String sourceTopicName = String.format("%s-source-%s", prefix, UUID.randomUUID());
    String sinkTopicName = String.format("%s-sink-%s", prefix, UUID.randomUUID());

    String testSubscriptionName = String.format("%s-test-%s", prefix, UUID.randomUUID());
    String pipelineReadSubscriptionName = String.format("%s-pipeline-%s", prefix, UUID.randomUUID());
    markSubscriptionForCleanup(pipelineReadSubscriptionName);

    ProjectTopicName sourceTopicNamePTN = createTopic(sourceTopicName);
    createTopic(sinkTopicName);

    ProjectSubscriptionName testSubscriptionPSN = createSubscription(sinkTopicName, testSubscriptionName);

    Subscriber subscriber = Subscriber.newBuilder(testSubscriptionPSN, receiver)
      .setCredentialsProvider(getCredentialProvider()).build();

    subscriber.addListener(new Subscriber.Listener() {
      public void failed(Subscriber.State from, Throwable failure) {
        LOG.error("State {}:", from.toString(), failure);
      }
    }, MoreExecutors.directExecutor());

    subscriber.startAsync().awaitRunning();

    Schema schema = RECORD_SCHEMA;

    if (sourceFormat == null || sourceFormat.equalsIgnoreCase(TEXT) || sourceFormat.equalsIgnoreCase(BLOB)) {
      schema = DEFAULT_SCHEMA;
    }

    final Map<String, String> sourceConfig = createSourceConfig(sourceTopicName, pipelineReadSubscriptionName,
                                                                sourceFormat, schema.toString(), delimiter);
    ETLStage ss = createSubscriberStage(sourceTopicName, sourceConfig);
    final Map<String, String> sinkConfig = createSinkConfig(sinkTopicName, sinkFormat, delimiter);
    ETLStage ps = createPublisherStage(sinkTopicName, sinkConfig);

    DataStreamsConfig config = DataStreamsConfig.builder()
      .addStage(ss)
      .addStage(ps)
      .addConnection(ss.getName(), ps.getName())
      .setBatchInterval("30s")
      .setStopGracefully(false)
      .build();

    ApplicationId appId = TEST_NAMESPACE.app(String.format("PubSubTestFormat-%s-to-%s", sourceFormat, sinkFormat));
    AppRequest<DataStreamsConfig> appRequest = getStreamingAppRequest(config);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    try {
      // it takes time to spin-up dataproc cluster, lets wait a little bit
      startAndWaitForRun(sparkManager, ProgramRunStatus.RUNNING,
                         Collections.singletonMap("system.profile.name", getProfileName()), 10, TimeUnit.MINUTES);

      // wait and check for source subscription to be created
      ensureSubscriptionCreated(pipelineReadSubscriptionName, 10, TimeUnit.MINUTES);

      // send some messages...
      publishByteMessages(sourceTopicNamePTN, messages);
      // ... and ensure they are passed through pipeline to our subscriber
      receiver.assertRetrievedMessagesContain(10, TimeUnit.MINUTES, expectedMessage);

      subscriber.stopAsync().awaitTerminated();
    } finally {
      try {
        sparkManager.stop();
        sparkManager.waitForStopped(5, TimeUnit.MINUTES);
      } catch (Exception e) {
        // don't treat this as a test failure, but log a warning
        LOG.warn("Pipeline failed to stop gracefully", e);
      }
    }
  }

  private Map<String, String> createSourceConfig(String topicName, String subscription, String format, String schema,
                                                 String delimiter) {
    final ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<>();
    configBuilder.put(PROJECT, getProjectId())
      .put(TOPIC, topicName)
      .put(REFERENCE_NAME, "pubsub-input")
      .put(SUBSCRIPTION, subscription);
    if (!Strings.isEmpty(format)) {
      configBuilder.put(FORMAT, format);
    }
    if (!Strings.isEmpty(schema)) {
      configBuilder.put(SCHEMA, schema);
    }
    if (!Strings.isEmpty(delimiter)) {
      configBuilder.put(DELIMITER, delimiter);
    }
    return configBuilder.build();
  }

  private Map<String, String> createSinkConfig(String topicName, String format, String delimiter) {
    final ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<>();
    configBuilder.put(PROJECT, getProjectId())
      .put(TOPIC, topicName)
      .put(REFERENCE_NAME, "pubsub-output");

    if (!Strings.isEmpty(format)) {
      configBuilder.put(FORMAT, format);
    }
    if (!Strings.isEmpty(delimiter)) {
      configBuilder.put(DELIMITER, delimiter);
    }
    return configBuilder.build();
  }

  private void ensureSubscriptionCreated(String subscriptionName, long timeout, TimeUnit unit) throws Exception {
    ProjectSubscriptionName name = ProjectSubscriptionName.of(getProjectId(), subscriptionName);
    Tasks.waitFor(true, () -> {
      try {
        subscriptionAdmin.getSubscription(name);
        LOG.info("Subscription {} created by pipeline in a timely manner", name.toString());
        return true;
      } catch (NotFoundException ex) {
        return false;
      }
    }, timeout, unit);
  }

  private void publishMessages(ProjectTopicName topicName, String... messages) throws Exception {
    Publisher publisher = Publisher.newBuilder(topicName).setCredentialsProvider(getCredentialProvider()).build();
    for (String message : messages) {
      ByteString data = ByteString.copyFromUtf8(message);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
      publisher.publish(pubsubMessage);
    }
    publisher.shutdown();
  }

  private void publishByteMessages(ProjectTopicName topicName, ByteString... messages) throws Exception {
    Publisher publisher = Publisher.newBuilder(topicName).setCredentialsProvider(getCredentialProvider()).build();
    for (ByteString message : messages) {
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(message).build();
      publisher.publish(pubsubMessage);
    }
    publisher.shutdown();
  }

  private ETLStage createSubscriberStage(String topicName, String subscription) {
    return createSubscriberStage(topicName, ImmutableMap.of(PROJECT, getProjectId(),
                                                            TOPIC, topicName,
                                                            REFERENCE_NAME, "pubsub-input",
                                                            SUBSCRIPTION, subscription));
  }

  private ETLStage createSubscriberStage(String topicName, Map<String, String> config) {
    return new ETLStage(topicName, new ETLPlugin(GOOGLE_SUBSCRIBER_PLUGIN_NAME, StreamingSource.PLUGIN_TYPE,
                                                 config, GOOGLE_CLOUD_ARTIFACT));
  }

  private ETLStage createPublisherStage(String topicName) {
    return createPublisherStage(topicName, ImmutableMap.of(PROJECT, getProjectId(),
                                                           REFERENCE_NAME, "pubsub-output",
                                                           TOPIC, topicName));
  }

  private static ByteString genericRecordToByteString(org.apache.avro.Schema avroSchema,
                                                      GenericData.Record record) throws IOException {
    final byte[] serializedBytes;
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    datumWriter.write(record, encoder);
    encoder.flush();
    out.close();
    serializedBytes = out.toByteArray();
    return ByteString.copyFrom(serializedBytes);
  }

  private ETLStage createPublisherStage(String topicName, Map<String, String> config) {
    return new ETLStage(topicName, new ETLPlugin(GOOGLE_PUBLISHER_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, config,
                                                 GOOGLE_CLOUD_ARTIFACT));
  }

  private abstract static class PublisherMessageReceiver implements MessageReceiver {
    final List<Object> messages = new CopyOnWriteArrayList<>();
    final Gson gson = new Gson();

    void assertRetrievedMessagesContain(long timeout, TimeUnit unit, Object... expectedMessages) throws Exception {
      Tasks.waitFor(true, () -> messages.size() == expectedMessages.length, timeout, unit);
      Assert.assertEquals("Retrieved messages is not same as expected",
                          new HashSet<>(Arrays.asList(expectedMessages)),
                          new HashSet<>(messages));
    }
  }

  private static class TestMessageReceiver extends PublisherMessageReceiver {
    private final Gson gson = new Gson();

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      ackReplyConsumer.ack();
      String msgJson = pubsubMessage.getData().toStringUtf8();
      CDAPPubSubMessage msg = gson.fromJson(msgJson, CDAPPubSubMessage.class);
      String messageString = msg.getMessageString();
      messages.add(messageString);
      LOG.info("Retrieved PubSub message: {}", messageString);
    }

    static class CDAPPubSubMessage {
      private byte[] message;
      private String id;
      private long timestamp;
      private Map<String, String> attributes = new HashMap<String, String>();

      String getMessageString() {
        return new String(message, StandardCharsets.UTF_8);
      }
    }
  }

  private static class AvroAndParquetPublisherReceiver extends PublisherMessageReceiver {

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      consumer.ack();
      final byte[] payloadData = message.getData().toByteArray();
      final org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
        parse(RECORD_SCHEMA.toString());
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      ByteArrayInputStream in = new ByteArrayInputStream(payloadData);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
      try {
        GenericRecord record = datumReader.read(null, decoder);
        final GenericData.Record payload = (GenericData.Record) record.get("message");
        final org.apache.avro.Schema payloadSchema = new org.apache.avro.Schema.Parser().
          parse(MESSAGE_SCHEMA.toString());
        messages.add(PubSubTest.genericRecordToByteString(payloadSchema, payload));
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  private static class JsonPublisherMessageReceiver extends PublisherMessageReceiver {

    static class CDAPPubSubMessageJSON {
      private Map<String, String> message;
      private String id;
      private long timestamp;
      private Map<String, String> attributes = new HashMap<String, String>();

      Map<String, String> getMessageString() {
        return message;
      }
    }

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      ackReplyConsumer.ack();
      String msgJson = pubsubMessage.getData().toStringUtf8();
      CDAPPubSubMessageJSON msg = gson.fromJson(msgJson, CDAPPubSubMessageJSON.class);
      Map<String, String> messageString = msg.getMessageString();
      messages.add(gson.toJson(messageString));
    }
  }

  private static class DelimitedSourceToJsonPublisherMessageReceiver extends PublisherMessageReceiver {

    static class CDAPPubSubMessageJSON {
      private Map<String, String> message;
      private String id;
      private long timestamp;
      private Map<String, String> attributes = new HashMap<String, String>();

      Map<String, String> getMessageString() {
        return message;
      }
    }

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      ackReplyConsumer.ack();
      String msgJson = pubsubMessage.getData().toStringUtf8();
      CDAPPubSubMessageJSON msg = gson.fromJson(msgJson, CDAPPubSubMessageJSON.class);
      Map<String, String> messageString = msg.getMessageString();
      final String message = messageString.get("message");
      messages.add(message);
    }
  }

  private static class DelimitedPublisherMessageReceiver extends PublisherMessageReceiver {
    private final String delimiter;

    DelimitedPublisherMessageReceiver(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      ackReplyConsumer.ack();
      String msgJson = pubsubMessage.getData().toStringUtf8();
      final String[] split = msgJson.split(delimiter);
      final String message = split[0];
      messages.add(message);
    }
  }
}
