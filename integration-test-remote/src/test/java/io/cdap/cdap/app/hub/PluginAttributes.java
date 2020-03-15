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

package io.cdap.cdap.app.hub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java representation of the plugin json config file.
 */
public class PluginAttributes {
  private final List<String> parents;
  private final Map<String, String> properties;

  public PluginAttributes() {
    this.parents = new ArrayList<>();
    this.properties = new HashMap<>();
  }

  public List<String> getParents() {
    return parents;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
