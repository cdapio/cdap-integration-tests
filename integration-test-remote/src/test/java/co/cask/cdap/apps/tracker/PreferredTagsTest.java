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

package co.cask.cdap.apps.tracker;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.ValidateTagsResult;
import co.cask.tracker.TrackerService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;
import java.net.HttpURLConnection;
import java.net.URL;

import co.cask.tracker.AuditLogPublisher;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.ValidateTagsResult;
import co.cask.tracker.utils.ParameterCheck;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test various methods of preferred Tags.
 */
public class PreferredTagsTest extends AudiTestBase{
  private static final String TEST_JSON_TAGS = "[\"tag1\",\"tag2\",\"tag3\",\"ta*4\"]";
  private static final String DEMOTE_TAGS = "[\"tag1\"]";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestTrackerApp.class);
    RESTClient restClient = getRestClient();
    ServiceManager TrackService = applicationManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    TrackService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    FlowManager testFlowManager = applicationManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    StreamManager streamManager = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "testStream"));
    List<AuditMessage> testData = generateTestData();
    for (AuditMessage auditMessage : testData) {
      streamManager.send(GSON.toJson(auditMessage));
    }
    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(testData.size(), 60L, TimeUnit.SECONDS);


    URL serviceURL = TrackService.getServiceURL();
    URL url_promote = new URL(serviceURL, "v1/tags/promote");
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.post(url_promote).withBody(TEST_JSON_TAGS).build());

    URL url_getTags = new URL(serviceURL, "v1/tags?type=preferred");
    HttpResponse response = restClient.execute(HttpRequest.get(url_getTags).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    TagsResult getTagsResult = GSON.fromJson(response.getResponseBodyAsString(), TagsResult.class);
    Assert.assertEquals(3, getTagsResult.getPreferred());

    URL url_demote = new URL(serviceURL, "v1/tags/demote");
    response = restClient.execute(HttpRequest.post(url_demote).withBody(DEMOTE_TAGS).build(), getClientConfig().getAccessToken());
    HttpResponse demoteResponse = restClient.execute(HttpRequest.get(url_getTags).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, demoteResponse.getResponseCode());
    TagsResult tagsResult = GSON.fromJson(demoteResponse.getResponseBodyAsString(), TagsResult.class);
    Assert.assertEquals(2, tagsResult.getPreferred());


    URL url_delete = new URL(serviceURL, "v1/tags/preferred?tag=tag2");
    response = restClient.execute(HttpRequest.delete(url_delete).build(), getClientConfig().getAccessToken());
    HttpResponse deleteResponse = restClient.execute(HttpRequest.get(url_getTags).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, deleteResponse.getResponseCode());
    TagsResult result = GSON.fromJson(deleteResponse.getResponseBodyAsString(), TagsResult.class);
    Assert.assertEquals(1, result.getPreferred());

    URL url_validate = new URL(serviceURL, "v1/tags/validate");
    response = restClient.execute(HttpRequest.post(url_validate).withBody(TEST_JSON_TAGS).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    ValidateTagsResult validateTagsResult = GSON.fromJson(response.getResponseBodyAsString(), ValidateTagsResult.class);
    Assert.assertEquals(3, validateTagsResult.getValid());
    Assert.assertEquals(1, validateTagsResult.getInvalid());

    URL url_invalidateDataError = new URL(serviceURL, "auditlog/stream/stream1?startTime=1&endTime=0");
    //?how to allow error code?
    response = restClient.execute(HttpRequest.get(url_invalidateDataError).build(), getClientConfig().getAccessToken(), HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(),response.getResponseCode());
//    Assert.assertEquals(ParameterCheck.STARTTIME_GREATER_THAN_ENDTIME, response);

  }



  private List<AuditMessage> generateTestData() {
    List<AuditMessage> testData = new ArrayList<>();
    testData.add(new AuditMessage(1456956659461L,
                                  NamespaceId.DEFAULT.stream("stream1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program_run:ns1.app2.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659469L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    String metadataPayload = "{ \"previous\": { \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, " +
      "\"tags\": [ \"ut1\", \"ut2\" ] }, \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, " +
      "\"additions\": { \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, " +
      "\"deletions\": { \"USER\": { \"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } }";
    MetadataPayload payload = GSON.fromJson(metadataPayload, MetadataPayload.class);
    testData.add(new AuditMessage(1456956659470L,
                                  EntityId.fromString("application:default.app1"),
                                  "user1",
                                  AuditType.METADATA_CHANGE,
                                  payload)
    );
    testData.add(new AuditMessage(1456956659471L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659472L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659473L,
                                  EntityId.fromString("dataset:default.ds6"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659468L,
                                  NamespaceId.DEFAULT.stream("strm123"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program_run:ns1.app1.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659460L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659502L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659500L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659504L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.UNKNOWN,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659505L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659506L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.a.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659507L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    return testData;
  }
}
