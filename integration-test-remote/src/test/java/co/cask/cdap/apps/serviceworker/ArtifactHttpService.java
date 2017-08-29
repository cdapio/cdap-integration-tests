/*
 * Copyright 2017 Cask, Inc.
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

package co.cask.cdap.apps.serviceworker;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.conf.PluginClassDeserializer;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Artifact HttpService list artifacts and test endpoint to load artifact class using an artifact
 */
public class ArtifactHttpService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactHttpService.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();

  @Override
  protected void configure() {
    setName(ServiceApplication.ARTIFACT_SERVICE_NAME);
    addHandler(new ArtifactHandler());
  }

  public class ArtifactHandler extends AbstractHttpServiceHandler {

    @Path("list")
    @GET
    public void list(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      responder.sendJson(200, getContext().listArtifacts(), ARTIFACT_INFO_LIST_TYPE, GSON);
    }

    @Path("test-artifact/{artifact-name}/load/{class-name}")
    @GET
    public void testArtifact(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("artifact-name") String artifactName,
                             @PathParam("class-name") String className) throws IOException {
      List<ArtifactInfo> artifactInfoList = getContext().listArtifacts();
      for (ArtifactInfo artifactInfo : artifactInfoList) {
        if (artifactInfo.getName().equals(artifactName)) {
          try (CloseableClassLoader classLoader = getContext().createClassLoader(artifactInfo, null)) {
            classLoader.loadClass(className);
            responder.sendJson(200, "Successfully loaded class " + className);
            return;
          } catch (ClassNotFoundException e) {
            LOG.error("Exception loading class {} using artifact {}", className, artifactName, e);
            responder.sendError(500,
                                String.format("Exception while loading class %s - %s.", className, e.getMessage()));
            return;
          }
        }
      }
      responder.sendError(400, "Artifact not found");
    }
  }
}
