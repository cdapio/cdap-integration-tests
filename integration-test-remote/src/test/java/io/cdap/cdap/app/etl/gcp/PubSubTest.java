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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
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

public class PubSubTest extends DataprocETLTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubTest.class);
  private static final String GOOGLE_SUBSCRIBER_PLUGIN_NAME = "GoogleSubscriber";
  private static final String GOOGLE_PUBLISHER_PLUGIN_NAME = "GooglePublisher";

  private static TopicAdminClient topicAdmin;
  private static SubscriptionAdminClient subscriptionAdmin;
  private ArrayList<ProjectTopicName> topicsToDelete;
  private ArrayList<ProjectSubscriptionName> subscriptionsToDelete;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    topicAdmin = TopicAdminClient.create(
      TopicAdminSettings.newBuilder().setCredentialsProvider(getCredentialProvider()).build());
    subscriptionAdmin = SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder().setCredentialsProvider(getCredentialProvider()).build());
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
      sparkManager.startAndWaitForRun(Collections.singletonMap("system.profile.name", getProfileName()),
                                      ProgramRunStatus.RUNNING, 10, TimeUnit.MINUTES);

      // wait and check for source subscription to be created
      ensureSubscriptionCreated(pipelineReadSubscriptionName, 10, TimeUnit.MINUTES);

      // send some messages...
      publishMessages(sourceTopicNamePTN, "message1", "message3", "message2");
      // ... and ensure they are passed through pipeline to our subscriber
      receiver.assertRetrievedMessagesContain(30, TimeUnit.MINUTES, "message1", "message3", "message2");

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

  private ETLStage createSubscriberStage(String topicName, String subscription) {
    return new ETLStage(topicName, new ETLPlugin(GOOGLE_SUBSCRIBER_PLUGIN_NAME, StreamingSource.PLUGIN_TYPE,
                                                 ImmutableMap.of("project", getProjectId(),
                                                                 "topic", topicName,
                                                                 "referenceName", "pubsub-input",
                                                                 "subscription", subscription), GOOGLE_CLOUD_ARTIFACT));
  }

  private ETLStage createPublisherStage(String topicName) {
    return new ETLStage(topicName, new ETLPlugin(GOOGLE_PUBLISHER_PLUGIN_NAME, BatchSink.PLUGIN_TYPE,
                                                 ImmutableMap.of("project", getProjectId(),
                                                                 "referenceName", "pubsub-output",
                                                                 "topic", topicName), GOOGLE_CLOUD_ARTIFACT));
  }

  private static class TestMessageReceiver implements MessageReceiver {
    private List<String> messages = new CopyOnWriteArrayList<>();
    private Gson gson = new Gson();

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      ackReplyConsumer.ack();
      String msgJson = pubsubMessage.getData().toStringUtf8();
      CDAPPubSubMessage msg = gson.fromJson(msgJson, CDAPPubSubMessage.class);
      String messageString = msg.getMessageString();
      messages.add(messageString);
      LOG.info("Retrieved PubSub message: {}", messageString);
    }

    void assertRetrievedMessagesContain(long timeout, TimeUnit unit, String... expectedMessages) throws Exception {
      Tasks.waitFor(true, () -> messages.size() == expectedMessages.length, timeout, unit);
      Assert.assertEquals("Retrieved messages is not same as expected",
                          new HashSet<>(Arrays.asList(expectedMessages)),
                          new HashSet<>(messages));
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
}
