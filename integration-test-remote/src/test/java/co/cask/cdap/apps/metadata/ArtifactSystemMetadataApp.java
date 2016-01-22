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

package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.plugin.PluginProperties;

/**
 * Dummy app with a plugins to test system metadata for artifacts.
 */
public class ArtifactSystemMetadataApp extends AbstractApplication {

  static final String PLUGIN_TYPE = "echo";
  static final String PLUGIN1_NAME = "echoplugin1";
  static final String PLUGIN2_NAME = "echoplugin2";

  @Override
  public void configure() {
    usePlugin(PLUGIN_TYPE, PLUGIN1_NAME, "p1", PluginProperties.builder().build());
    usePlugin(PLUGIN_TYPE, PLUGIN2_NAME, "p2", PluginProperties.builder().build());
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN1_NAME)
  @SuppressWarnings("unused")
  public static class EchoPlugin1 {
    public void test() {
      System.out.println(PLUGIN1_NAME);
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN2_NAME)
  @SuppressWarnings("unused")
  public static class EchoPlugin2 {
    public void test() {
      System.out.println(PLUGIN2_NAME);
    }
  }
}
