/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.etl.wrangler;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for wrangler service.
 */
public class WranglerServiceTest extends ETLTestBase {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final String WORKSPACE_NAME = "test_ws";

  public class WranglerServiceRequest {
    private final double version;
    private final Workspace workspace;
    private final Recipe recipe;
    private final Sampling sampling;

    public WranglerServiceRequest(double version, Workspace workspace, Recipe recipe, Sampling sampling) {
      this.version = version;
      this.workspace = workspace;
      this.recipe = recipe;
      this.sampling = sampling;
    }
  }

  private class Workspace {
    private final String name;
    private final int results;

    public Workspace(String name, int results) {
      this.name = name;
      this.results = results;
    }
  }

  private class Recipe {
    private final List<String> directives;
    private final boolean save;
    private final String name;

    public Recipe(List<String> directives, boolean save, String name) {
      this.directives = directives;
      this.save = save;
      this.name = name;
    }
  }

  private class Sampling {
    private final String method;
    private final int seed;
    private final int limit;

    public Sampling(String method, int seed, int limit) {
      this.method = method;
      this.seed = seed;
      this.limit = limit;
    }
  }

  @Test
  public void test() throws Exception {

    ApplicationId appId = TEST_NAMESPACE.app("dataprep");
    List<ArtifactSummary> artifactSummaryList = artifactClient.list(TEST_NAMESPACE.getNamespaceId(),
                                                                    ArtifactScope.SYSTEM);
    AppRequest appRequest = getWranglerAppRequest(artifactSummaryList);
    ApplicationManager appManager = deployApplication(appId, appRequest);
    ServiceManager wranglerServiceManager = appManager.getServiceManager("service");
    wranglerServiceManager.start();

    URL baseURL = wranglerServiceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    List<String> uploadContents = ImmutableList.of("bob,anderson", "joe,mchall");
    createAndUploadWorkspace(baseURL, WORKSPACE_NAME, uploadContents);

    Schema schema = schema(baseURL, WORKSPACE_NAME);
    Schema expectedSchema =
      Schema.recordOf("avroSchema",
                      Schema.Field.of("fname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("lname", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Assert.assertEquals(expectedSchema, schema);
    wranglerServiceManager.stop();
  }

  protected void createAndUploadWorkspace(URL baseURL, String workspace, List<String> lines) throws Exception {

    HttpResponse response = getRestClient().execute(HttpMethod.PUT, new URL(baseURL, "workspaces/" + workspace),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    String body = Joiner.on(URLEncoder.encode("\n", StandardCharsets.UTF_8.name())).join(lines);
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("recorddelimiter", URLEncoder.encode("\n", StandardCharsets.UTF_8.name()));
    response = getRestClient().execute(HttpMethod.POST, new URL(baseURL, "workspaces/" + workspace + "/upload"),
                                       body, headers, getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
  }

  public Schema schema(URL baseURL, String workspace) throws Exception {

    URL url = new URL(baseURL, "workspaces/" + workspace + "/schema");
   HttpResponse response = getRestClient().execute(HttpMethod.POST, url,
                                                   GSON.toJson(createServiceRequest()),
                                                   ImmutableMap.<String, String>of(),
                                                   getClientConfig().getAccessToken());
    //verify that request has succeeded
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // we have to do this, simply because of how the service REST API returns only the Fields of the Schema
    return GSON.fromJson("{ \"name\": \"avroSchema\", \"type\": \"record\", \"fields\":"
                           + response.getResponseBodyAsString() + " }", Schema.class);
  }

  protected WranglerServiceRequest createServiceRequest() {
    Workspace workspace = new Workspace(WORKSPACE_NAME, 2);
    List<String> directives =
      ImmutableList.of("split-to-columns test_ws ,",
                       "drop test_ws",
                       "rename test_ws_1 fname",
                       "rename test_ws_2 lname");
    Recipe recipe = new Recipe(directives, true, "my-recipe");
    Sampling sampling = new Sampling("first", 1, 2);
    WranglerServiceRequest request = new WranglerServiceRequest(1.0, workspace, recipe, sampling);
    return request;
  }
}
