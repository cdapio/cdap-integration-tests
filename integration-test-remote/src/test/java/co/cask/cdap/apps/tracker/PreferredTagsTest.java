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

  }
}
