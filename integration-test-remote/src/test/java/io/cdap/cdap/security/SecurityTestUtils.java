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
package io.cdap.cdap.security;

import com.google.common.base.Preconditions;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ConfigEntry;
import org.apache.hadoop.security.authentication.util.KerberosName;

import java.util.Map;

/**
 * Utility class for Security ITN
 */
class SecurityTestUtils {
  static String getKeytabURIforPrincipal(String principal, Map<String, ConfigEntry> cdapConfig) throws Exception {
    // Get the configured keytab path from cdap-site.xml
    ConfigEntry configEntry = cdapConfig.get(Constants.Security.KEYTAB_PATH);
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s", Constants.Security.KEYTAB_PATH);
    String name = new KerberosName(principal).getShortName();
    // replace Constants.USER_NAME_SPECIFIER with the given name
    return configEntry.getValue().replace(Constants.USER_NAME_SPECIFIER, name);
  }
}
