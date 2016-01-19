/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.remote.dataset;

import co.cask.cdap.api.common.Bytes;
import com.google.gson.InstanceCreator;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;

/**
 * TypeAdapter that deserializes Maps into TreeMaps, with the {@link Bytes#BYTES_COMPARATOR}.
 */
public final class TreeMapInstanceCreator implements InstanceCreator<Map<?, ?>> {
  public TreeMapInstanceCreator() {
  }

  @Override
  public Map<?, ?> createInstance(Type type) {
    return new TreeMap<>(Bytes.BYTES_COMPARATOR);
  }
}
