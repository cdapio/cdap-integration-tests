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

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.Id;
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
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.TrackerService;
import co.cask.tracker.config.AuditLogKafkaConfig;
import co.cask.tracker.config.TrackerAppConfig;
import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.AuditLogResponse;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.TrackerMeterRequest;
import co.cask.tracker.entity.TrackerMeterResult;
import co.cask.tracker.entity.ValidateTagsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TrackerTestBase
 */
public class TrackerTestBase extends AudiTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  RESTClient restClient = getRestClient();
  private static Type datasetList = new TypeToken<List<TopDatasetsResult>>() { }.getType();
  private static Type programList = new TypeToken<List<TopProgramsResult>>() { }.getType();
  private static Type applicationList = new TypeToken<List<TopApplicationsResult>>() { }.getType();
  private static Type timesinceMap = new TypeToken<Map<String, Long>>() { }.getRawType();
  private static ServiceManager trackerService;
  private static FlowManager trackerFlow;
  private static StreamManager trackerStream;

  private URL serviceURL;

  protected void enableTracker() throws InterruptedException, IOException, UnauthenticatedException {
    String zookeeperQuorum = getMetaClient().getCDAPConfig().get(Constants.Zookeeper.QUORUM).getValue();
    ApplicationManager applicationManager = deployApplication(Id.Namespace.DEFAULT, TestTrackerApp.class,
                                                              new TrackerAppConfig(new AuditLogKafkaConfig(
                                                                zookeeperQuorum,
                                                                null, null, 0, "offsetDataset")));
    trackerService = applicationManager.getServiceManager(TrackerService.SERVICE_NAME).start();

    trackerService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerFlow = applicationManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    trackerFlow.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "testStream"));
    serviceURL = trackerService.getServiceURL();
  }

  protected void waitforProcessed(long count) throws TimeoutException, InterruptedException {
    RuntimeMetrics metrics = trackerFlow.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(count, 2L, TimeUnit.MINUTES);
  }

  protected void promoteTags(String tags) throws Exception {
    URL urlPromote = new URL(serviceURL, "v1/tags/promote");
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.post(urlPromote).withBody(tags).build());
  }

  protected TagsResult getPreferredTags() throws Exception {
    URL urlGetTags = new URL(serviceURL, "v1/tags?type=preferred");
    HttpResponse response = restClient.execute(HttpRequest.get(urlGetTags).build(),
                                               getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), TagsResult.class);
  }

  protected void sendTestAuditMessages(List<AuditMessage> messages) throws IOException {
    for (AuditMessage auditMessage : messages) {
      trackerStream.send(GSON.toJson(auditMessage));
    }
  }

  protected void demoteTags(String tagsToDemote) throws IOException, UnauthenticatedException {
    URL urlDemote = new URL(serviceURL, "v1/tags/demote");
    restClient.execute(HttpRequest.post(urlDemote).withBody(tagsToDemote).build(), getClientConfig().getAccessToken());
  }

  protected void deleteTags(String tagsToDelete) throws IOException, UnauthenticatedException {
    URL urlDelete = new URL(serviceURL, String.format("v1/tags/preferred?tag=%s", tagsToDelete));
    restClient.execute(HttpRequest.delete(urlDelete).build(), getClientConfig().getAccessToken());
  }

  protected ValidateTagsResult validateTags(String tagsToValidate) throws IOException, UnauthenticatedException {
    URL urlValidate = new URL(serviceURL, "v1/tags/validate");
    HttpResponse validateResponse = restClient.execute(HttpRequest.post(urlValidate).withBody(tagsToValidate).build(),
                                                       getClientConfig().getAccessToken());
    ValidateTagsResult validateTagsResult = GSON.fromJson(validateResponse.getResponseBodyAsString(),
                                                          ValidateTagsResult.class);
    return validateTagsResult;
  }

  protected List<TopDatasetsResult> getTopNDatasets() throws IOException, UnauthenticatedException {
    URL urlTopNDataset = new URL(serviceURL, "v1/auditmetrics/top-entities/datasets?limit=20");
    HttpResponse datasetResponse = restClient.execute(HttpRequest.get(urlTopNDataset).build(),
                                                      getClientConfig().getAccessToken());
    return GSON.fromJson(datasetResponse.getResponseBodyAsString(), datasetList);
  }

  protected List<TopProgramsResult> getTopNPrograms() throws IOException, UnauthenticatedException {
    URL urlTopNPrograms = new URL(serviceURL, "v1/auditmetrics/top-entities/programs?limit=20");
    HttpResponse programsResponse = restClient.execute(HttpRequest.get(urlTopNPrograms).build(),
                                                       getClientConfig().getAccessToken());
    return GSON.fromJson(programsResponse.getResponseBodyAsString(), programList);
  }

  protected List<TopApplicationsResult> getTopNApplication() throws IOException, UnauthenticatedException {
    URL urlTopNApplication = new URL(serviceURL, "v1/auditmetrics/top-entities/applications?limit=20");
    HttpResponse applicationResponse = restClient.execute(HttpRequest.get(urlTopNApplication).build(),
                                                          getClientConfig().getAccessToken());
    return GSON.fromJson(applicationResponse.getResponseBodyAsString(), applicationList);
  }

  protected Map<String, Long> getTimeSince() throws IOException, UnauthenticatedException {
    URL urlTimeSince = new URL(serviceURL, "v1/auditmetrics/time-since?entityType=dataset&entityName=ds1");
    HttpResponse timeSinceResponse = restClient.execute(HttpRequest.get(urlTimeSince).build(),
                                  getClientConfig().getAccessToken());
    return GSON.fromJson(timeSinceResponse.getResponseBodyAsString(), timesinceMap);
  }

  protected AuditHistogramResult getGlobalAuditLogHistogram() throws IOException, UnauthenticatedException {
    URL urlAuditLogHistogram = new URL(serviceURL, "v1/auditmetrics/audit-histogram");
    HttpResponse audiLogHisResponse = restClient.execute(HttpRequest.get(urlAuditLogHistogram).build(),
                                                          getClientConfig().getAccessToken());
    return GSON.fromJson(audiLogHisResponse.getResponseBodyAsString(), AuditHistogramResult.class);
  }


  protected AuditHistogramResult getSpecificAuditLogHistogram() throws IOException, UnauthenticatedException {
    URL urlAuditLogHistogram = new URL(serviceURL, "v1/auditmetrics/audit-histogram?entityType=dataset&entityName=ds1");
    HttpResponse audiLogHisResponse = restClient.execute(HttpRequest.get(urlAuditLogHistogram).build(),
                                                         getClientConfig().getAccessToken());
    return GSON.fromJson(audiLogHisResponse.getResponseBodyAsString(), AuditHistogramResult.class);
  }


  protected AuditLogResponse getAuditLog() throws IOException, UnauthenticatedException {
    URL urlAuditLog = new URL (serviceURL, "auditlog/stream/stream1");
    HttpResponse auditLogResponse = restClient.execute(HttpRequest.get(urlAuditLog)
                                                         .build(), getClientConfig().getAccessToken());
    return GSON.fromJson(auditLogResponse.getResponseBodyAsString(), AuditLogResponse.class);
  }

  protected TrackerMeterResult getTruthMeter() throws IOException, UnauthenticatedException {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds1");
    datasets.add("ds6");
    datasets.add("ds8");
    streams.add("strm123");
    return getTrackerMeterResponse(datasets, streams);
  }

  protected  TrackerMeterResult getTimeRank() throws IOException, UnauthenticatedException {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds6");
    datasets.add("ds8");
    datasets.add("ds9");
    datasets.add("ds1");
    streams.add("strm123");
    streams.add("stream1");
    return getTrackerMeterResponse(datasets, streams);
  }

  protected TrackerMeterResult getInvalidName() throws IOException, UnauthenticatedException {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds_invalid");
    datasets.add("ds_does_not_exit");
    datasets.add("ds_test");
    streams.add("strm_test");
    return getTrackerMeterResponse(datasets, streams);
  }

  private TrackerMeterResult getTrackerMeterResponse(List<String> datasets,
                                                     List<String> streams)
    throws IOException, UnauthenticatedException {
    URL urlTrakerMeter = new URL (serviceURL, "v1/tracker-meter");
    HttpResponse response = restClient.execute(HttpRequest.post(urlTrakerMeter).
                                                    withBody(GSON.toJson(new TrackerMeterRequest(datasets, streams)))
                                                         .build(), getClientConfig().getAccessToken());

    return GSON.fromJson(response.getResponseBodyAsString(), TrackerMeterResult.class);

  }

  protected void addEntityTags(Set<String> tagSet, String entityType, String entityName)
                                                                throws IOException, UnauthenticatedException {
    String url = String.format("v1/tags/promote/%s/%s", entityType, entityName);
    URL urladdEntityTags = new URL (serviceURL, url);
    restClient.execute(HttpRequest.post(urladdEntityTags).withBody(GSON.toJson(tagSet))
                                                 .build(), getClientConfig().getAccessToken());
  }

  protected void deleteEntityTags(Set<String> tagSet, String entityType, String entityName)
                                                                throws IOException, UnauthenticatedException {
    for (String tag : tagSet) {
      String url = String.format("v1/tags/delete/%s/%s?tagname=%s", entityType, entityName, tag);
      URL urldeleteEntityTags = new URL (serviceURL, url);
      restClient.execute(HttpRequest.delete(urldeleteEntityTags)
                           .build(), getClientConfig().getAccessToken());
    }
  }

  protected TagsResult getEntityTags(String entityType, String entityName)
                                                                throws IOException, UnauthenticatedException {
    String url = String.format("v1/tags/%s/%s", entityType, entityName);
    URL urlgetEntityTags = new URL (serviceURL, url);
    HttpResponse response = restClient.execute(HttpRequest.get(urlgetEntityTags)
                                                 .build(), getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), TagsResult.class);
  }

  protected Set<String> generateStringList(int maxStringLength, String characters, int stringNum) {
    Random rng = new Random();
    Set<String> set = new HashSet<>();
    for (int i = 0; i < stringNum; i++) {
      set.add(generateString(rng, characters, maxStringLength));
    }
    return set;
  }

  private String generateString (Random rng, String characters, int maxLength) {
    int length = rng.nextInt(maxLength) + 1;
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = characters.charAt(rng.nextInt(characters.length()));
    }
    return new String(text);
  }


  protected List<AuditMessage> generateTestData() {
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
    testData.add(new AuditMessage(1456956659509L,
                                  NamespaceId.DEFAULT.dataset("ds8"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659507L,
                                  NamespaceId.DEFAULT.dataset("ds9"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659471L,
                                  EntityId.fromString("dataset:default.ds5"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    return testData;
  }
}
