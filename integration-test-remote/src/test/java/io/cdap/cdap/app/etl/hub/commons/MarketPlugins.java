/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.hub.commons;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.InvalidArtifactRangeException;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.id.ArtifactId;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities for market plugins.
 */
public final class MarketPlugins {

  public static final String PACKAGES_JSON_URL = "https://hub-cdap-io.storage.googleapis.com/v2/packages.json";
  public static final String PLUGIN_RESOURCE_BASE_URL = "https://hub-cdap-io.storage.googleapis.com/v2/packages/%s/%s/%s-%s";

  private MarketPlugins() {

  }

  private static ArtifactRange parseArtifactString(String parentString) {
    String namespace = parentString.substring(0, parentString.indexOf(":"));
    int firstRangeIndex = parentString.indexOf("[");
    if (firstRangeIndex == -1) {
      firstRangeIndex = parentString.indexOf("(");
    }
    String name = parentString.substring(namespace.length() + 1, firstRangeIndex);
    String versionString = parentString.substring(firstRangeIndex);

    try {
      return new ArtifactRange(namespace, name, ArtifactRange.parse(versionString));
    } catch (InvalidArtifactRangeException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load a market plugin into this instance for integration testing.
   * @param artifactClient the artifact client available in all ETLTestBase subclasses
   * @param pluginName full name of the plugin used in the hub
   * @param pluginShortName shortname of the plugin (note that the jarfile is to be named shortname-version.jar)
   * @param namespaceName the namespace in which to load the plugin (can be default)
   * @throws UnauthenticatedException
   * @throws BadRequestException
   * @throws ArtifactRangeNotFoundException
   * @throws ArtifactAlreadyExistsException
   * @throws IOException
   * @return the ArtifactID that was loaded, or null if the artifact was not found
   */
  public static ArtifactId loadPlugin(ArtifactClient artifactClient, String pluginName, String pluginVersion,
    String pluginShortName, String namespaceName)
    throws UnauthenticatedException, BadRequestException, ArtifactRangeNotFoundException,
    ArtifactAlreadyExistsException, IOException {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet packagesJsonRequest = new HttpGet(PACKAGES_JSON_URL);

      try (CloseableHttpResponse packagesJsonResponse = httpClient.execute(packagesJsonRequest)) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<?, ?>> list = objectMapper.readValue(
          EntityUtils.toString(packagesJsonResponse.getEntity()), List.class);

        for (Map<?, ?> pluginMap : list) {
          if (pluginName.equals(pluginMap.get("name")) && pluginVersion.equals(pluginMap.get("version"))) {
            String baseUrl = String.format(PLUGIN_RESOURCE_BASE_URL,
              pluginName, pluginVersion, pluginShortName, pluginVersion);
            String jarUrl = baseUrl + ".jar";
            String jsonUrl = baseUrl + ".json";

            HttpGet pluginJsonRequest = new HttpGet(jsonUrl);

            // We need to also request the plugin.json file to get the parent plugin names. This is required for the
            // artifactClient to load the plugin; it can't do so without knowing the list of parents.
            try (CloseableHttpResponse pluginJsonResponse = httpClient.execute(pluginJsonRequest)) {
              Map<?, ?> metadata = objectMapper.readValue(
                EntityUtils.toString(pluginJsonResponse.getEntity()), Map.class);
              List<String> parentsList = (List<String>) metadata.get("parents");
              Set<ArtifactRange> parents = parentsList.stream().map(MarketPlugins::parseArtifactString)
                .collect(Collectors.toSet());

              HttpGet pluginJarRequest = new HttpGet(jarUrl);
              try (CloseableHttpResponse jarResponse = httpClient.execute(pluginJarRequest)) {
                byte[] array = EntityUtils.toByteArray(jarResponse.getEntity());
                ArtifactId artifactId = new ArtifactId(namespaceName, pluginShortName + "-" + pluginVersion + ".jar");
                artifactClient.add(artifactId, parents, () -> new ByteArrayInputStream(array));
                return artifactId;
              }
            }
          }
        }
      }
    }
    return null;
  }
}
