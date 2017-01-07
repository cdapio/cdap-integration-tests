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
import co.cask.cdap.proto.id.SystemServiceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.TrackerApp;
import co.cask.tracker.TrackerService;
import co.cask.tracker.config.AuditLogConfig;
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
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
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
  private static final int SEED = 0;
  RESTClient restClient = getRestClient();
  private static Type datasetList = new TypeToken<List<TopDatasetsResult>>() { }.getType();
  private static Type programList = new TypeToken<List<TopProgramsResult>>() { }.getType();
  private static Type applicationList = new TypeToken<List<TopApplicationsResult>>() { }.getType();
  private static Type timesinceMap = new TypeToken<Map<String, Long>>() { }.getRawType();
  private static ServiceManager trackerService;
  private static FlowManager trackerFlow;
  private static StreamManager trackerStream;

  private URL serviceURL;

  protected void enableTracker()
    throws InterruptedException, IOException, UnauthenticatedException, UnauthorizedException {
    String zookeeperQuorum = getMetaClient().getCDAPConfig().get(Constants.Zookeeper.QUORUM).getValue();
    TrackerAppConfig appConfig =
      new TrackerAppConfig(new AuditLogConfig(zookeeperQuorum, null, null, null, null));
    ApplicationManager applicationManager = getTestManager().deployApplication(
      NamespaceId.DEFAULT, TestTrackerApp.class, appConfig);
    trackerService = applicationManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    trackerService.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerFlow = applicationManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    trackerFlow.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);
    trackerStream = getTestManager().getStreamManager(TEST_NAMESPACE.stream("testStream"));
    serviceURL = trackerService.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  protected void waitforProcessed(long count) throws TimeoutException, InterruptedException {
    RuntimeMetrics metrics = trackerFlow.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(count, 2L, TimeUnit.MINUTES);
  }

  protected void promoteTags(String tags) throws Exception {
    URL urlPromote = new URL(serviceURL, "v1/tags/promote");
    HttpResponse response = restClient.execute(HttpRequest.post(urlPromote).withBody(tags).build(),
                                               getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
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

  protected void demoteTags(String tagsToDemote) throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlDemote = new URL(serviceURL, "v1/tags/demote");
    restClient.execute(HttpRequest.post(urlDemote).withBody(tagsToDemote).build(), getClientConfig().getAccessToken());
  }

  protected void deleteTags(String tagsToDelete) throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlDelete = new URL(serviceURL, String.format("v1/tags/preferred?tag=%s", tagsToDelete));
    restClient.execute(HttpRequest.delete(urlDelete).build(), getClientConfig().getAccessToken());
  }

  protected ValidateTagsResult validateTags(String tagsToValidate)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlValidate = new URL(serviceURL, "v1/tags/validate");
    HttpResponse validateResponse = restClient.execute(HttpRequest.post(urlValidate).withBody(tagsToValidate).build(),
                                                       getClientConfig().getAccessToken());
    return GSON.fromJson(validateResponse.getResponseBodyAsString(), ValidateTagsResult.class);
  }

  protected List<TopDatasetsResult> getTopNDatasets()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlTopNDataset = new URL(serviceURL, "v1/auditmetrics/top-entities/datasets?limit=20");
    HttpResponse datasetResponse = restClient.execute(HttpRequest.get(urlTopNDataset).build(),
                                                      getClientConfig().getAccessToken());
    return GSON.fromJson(datasetResponse.getResponseBodyAsString(), datasetList);
  }

  protected List<TopProgramsResult> getTopNPrograms()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlTopNPrograms = new URL(serviceURL, "v1/auditmetrics/top-entities/programs?limit=20");
    HttpResponse programsResponse = restClient.execute(HttpRequest.get(urlTopNPrograms).build(),
                                                       getClientConfig().getAccessToken());
    return GSON.fromJson(programsResponse.getResponseBodyAsString(), programList);
  }

  protected List<TopApplicationsResult> getTopNApplication()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlTopNApplication = new URL(serviceURL, "v1/auditmetrics/top-entities/applications?limit=20");
    HttpResponse applicationResponse = restClient.execute(HttpRequest.get(urlTopNApplication).build(),
                                                          getClientConfig().getAccessToken());
    return GSON.fromJson(applicationResponse.getResponseBodyAsString(), applicationList);
  }

  protected Map<String, Long> getTimeSince() throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlTimeSince = new URL(serviceURL, "v1/auditmetrics/time-since?entityType=dataset&entityName=ds1");
    HttpResponse timeSinceResponse = restClient.execute(HttpRequest.get(urlTimeSince).build(),
                                                        getClientConfig().getAccessToken());
    return GSON.fromJson(timeSinceResponse.getResponseBodyAsString(), timesinceMap);
  }

  protected AuditHistogramResult getGlobalAuditLogHistogram()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlAuditLogHistogram = new URL(serviceURL, "v1/auditmetrics/audit-histogram");
    HttpResponse audiLogHisResponse = restClient.execute(HttpRequest.get(urlAuditLogHistogram).build(),
                                                         getClientConfig().getAccessToken());
    return GSON.fromJson(audiLogHisResponse.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected AuditHistogramResult getSpecificAuditLogHistogram()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlAuditLogHistogram = new URL(serviceURL, "v1/auditmetrics/audit-histogram?entityType=dataset&entityName=ds1");
    HttpResponse audiLogHisResponse = restClient.execute(HttpRequest.get(urlAuditLogHistogram).build(),
                                                         getClientConfig().getAccessToken());
    return GSON.fromJson(audiLogHisResponse.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected AuditLogResponse getAuditLog() throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlAuditLog = new URL (serviceURL, "v1/auditlog/stream/stream1");
    HttpResponse auditLogResponse = restClient.execute(HttpRequest.get(urlAuditLog).build(),
                                                       getClientConfig().getAccessToken());
    return GSON.fromJson(auditLogResponse.getResponseBodyAsString(), AuditLogResponse.class);
  }

  protected TrackerMeterResult getTruthMeter() throws IOException, UnauthenticatedException, UnauthorizedException {
    ImmutableList<String> datasets = ImmutableList.of("ds1", "ds6", "ds8");
    ImmutableList<String> streams = ImmutableList.of("strm123");
    return getTrackerMeterResponse(datasets, streams);
  }

  protected  TrackerMeterResult getTimeRank() throws IOException, UnauthenticatedException, UnauthorizedException {
    ImmutableList<String> datasets = ImmutableList.of("ds6", "ds8", "ds9", "ds1");
    ImmutableList<String> streams = ImmutableList.of("strm123", "stream1");
    return getTrackerMeterResponse(datasets, streams);
  }

  protected TrackerMeterResult getInvalidName() throws IOException, UnauthenticatedException, UnauthorizedException {
    ImmutableList<String> streams = ImmutableList.of("strm_test");
    ImmutableList<String> datasets = ImmutableList.of("ds_invalid", "ds_does_not_exit", "ds_test");
    return getTrackerMeterResponse(datasets, streams);
  }

  private TrackerMeterResult getTrackerMeterResponse(List<String> datasets,
                                                     List<String> streams)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlTrakerMeter = new URL (serviceURL, "v1/tracker-meter");
    HttpResponse response = restClient.execute(
      HttpRequest.post(urlTrakerMeter).withBody(GSON.toJson(new TrackerMeterRequest(datasets, streams))).build(),
      getClientConfig().getAccessToken());

    return GSON.fromJson(response.getResponseBodyAsString(), TrackerMeterResult.class);

  }

  protected AuditHistogramResult getDatasetFilter()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlDatasetFilter = new URL(serviceURL, "v1/auditmetrics/audit-histogram?entityType=dataset&entityName="
                                               + TrackerApp.AUDIT_LOG_DATASET_NAME);
    HttpResponse response = restClient.execute(HttpRequest.get(urlDatasetFilter).build(),
                                               getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected AuditHistogramResult getKafkaFilter() throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlKafkaFilter = new URL(serviceURL, "v1/auditmetrics/audit-histogram?entityType=dataset&entityName="
                                             + AuditLogConfig.DEFAULT_OFFSET_DATASET);
    HttpResponse response = restClient.execute(HttpRequest.get(urlKafkaFilter).build(),
                                               getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected List<TopProgramsResult> getProgramFilter()
    throws IOException, UnauthenticatedException, UnauthorizedException {
    URL urlPorgramFilter = new URL(serviceURL,
                                   "v1/auditmetrics/top-entities/programs?entityName=dsx&entityType=dataset");
    HttpResponse response = restClient.execute(HttpRequest.get(urlPorgramFilter).build(),
                                               getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), programList);
  }

  protected void addEntityTags(Set<String> tagSet, String entityType, String entityName)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    String url = String.format("v1/tags/promote/%s/%s", entityType, entityName);
    URL urladdEntityTags = new URL (serviceURL, url);
    restClient.execute(HttpRequest.post(urladdEntityTags).withBody(GSON.toJson(tagSet))
                                                         .build(), getClientConfig().getAccessToken());
  }

  protected void deleteEntityTags(Set<String> tagSet, String entityType, String entityName)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    for (String tag : tagSet) {
      String url = String.format("v1/tags/delete/%s/%s?tagname=%s", entityType, entityName, tag);
      URL urldeleteEntityTags = new URL (serviceURL, url);
      restClient.execute(HttpRequest.delete(urldeleteEntityTags)
                                    .build(), getClientConfig().getAccessToken());
    }
  }

  protected TagsResult getEntityTags(String entityType, String entityName)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    String url = String.format("v1/tags/%s/%s", entityType, entityName);
    URL urlgetEntityTags = new URL (serviceURL, url);
    HttpResponse response = restClient.execute(HttpRequest.get(urlgetEntityTags)
                                                          .build(), getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), TagsResult.class);
  }

  protected AuditHistogramResult getResolutionBucket(String startTime)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    String url = String.format("v1/auditmetrics/audit-histogram?entityType=dataset" +
                               "&entityName=ds1&startTime=%s&endTime=now", startTime);
    URL urlResolutionBucket = new URL (serviceURL, url);
    HttpResponse response = restClient.execute(HttpRequest.get(urlResolutionBucket)
                                                          .build(), getClientConfig().getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), AuditHistogramResult.class);
  }

  protected Set<String> generateStringList(int maxStringLength, String characters, int stringNum) {
    Random rng = new Random(SEED);
    Set<String> set = new HashSet<>();
    for (int i = 0; i < stringNum; i++) {
      set.add(generateString(rng, characters, maxStringLength));
    }
    return set;
  }

  private String generateString (Random rng, String characters, int maxLength) {
    int length = rng.nextInt(maxLength - 1) + 1;
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = characters.charAt(rng.nextInt(characters.length()));
    }
    return new String(text);
  }

  protected List<AuditMessage> generateTestData() {
    List<AuditMessage> testData = new ArrayList<>();
    NamespaceId ns1 = new NamespaceId("ns1");
    testData.add(new AuditMessage(1456956659461L,
                                  NamespaceId.DEFAULT.stream("stream1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app("app2").flow("flow1").run("run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659469L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, new SystemServiceId("explore"))
                 )
    );
    String metadataPayload = "{ \"previous\": { \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, " +
      "\"tags\": [ \"ut1\", \"ut2\" ] }, \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, " +
      "\"additions\": { \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, " +
      "\"deletions\": { \"USER\": { \"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } }";
    MetadataPayload payload = GSON.fromJson(metadataPayload, MetadataPayload.class);
    testData.add(new AuditMessage(1456956659470L,
                                  NamespaceId.DEFAULT.app("app1"),
                                  "user1",
                                  AuditType.METADATA_CHANGE,
                                  payload)
    );
    testData.add(new AuditMessage(1456956659471L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659472L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659473L,
                                  NamespaceId.DEFAULT.dataset("ds6"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659468L,
                                  NamespaceId.DEFAULT.stream("strm123"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, ns1.app("app1").flow("flow1").run("run1"))));
    testData.add(new AuditMessage(1456956659460L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, new SystemServiceId("explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659502L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, new SystemServiceId("explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659500L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, new SystemServiceId("explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659504L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.UNKNOWN, new SystemServiceId("explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659505L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app("b").service("program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659506L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app("a").service("program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659507L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, ns1.app("b").service("program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659509L,
                                  NamespaceId.DEFAULT.dataset("ds8"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, ns1.app("b").service("program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659511L,
                                  NamespaceId.DEFAULT.dataset("ds9"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ, ns1.app("b").service("program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659512L,
                                  NamespaceId.DEFAULT.dataset("ds5"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659513L,
                                  NamespaceId.DEFAULT.dataset(TrackerApp.AUDIT_LOG_DATASET_NAME),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app("b").service("program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659516L,
                                  NamespaceId.DEFAULT.dataset("dsx"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app(TrackerApp.APP_NAME).service("program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659513L,
                                  NamespaceId.DEFAULT.dataset(AuditLogConfig.DEFAULT_OFFSET_DATASET),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE, ns1.app("b").service("program1"))
                 )
    );
    return testData;
  }

}



