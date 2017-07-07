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

package co.cask.cdap.security;

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Authorization test base for all authorization tests
 */
public abstract class AuthorizationTestBase extends AudiTestBase {
  protected static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  protected static final String ALICE = "alice";
  protected static final String BOB = "bob";
  protected static final String CAROL = "carol";
  protected static final String EVE = "eve";
  protected static final String ADMIN_USER = "cdapitn";
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String VERSION = "1.0.0";
  protected static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";

  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  protected NamespaceId createAndRegisterNamespace(NamespaceMeta namespaceMeta, ClientConfig config,
                                                 RESTClient client) throws Exception {
    try {
      new NamespaceClient(config, client).create(namespaceMeta);
    } finally {
      registerForDeletion(namespaceMeta.getNamespaceId());
    }
    return namespaceMeta.getNamespaceId();
  }

  protected NamespaceMeta getNamespaceMeta(NamespaceId namespaceId, @Nullable String principal,
                                                  @Nullable String groupName, @Nullable String keytabURI,
                                                  @Nullable String rootDirectory, @Nullable String hbaseNamespace,
                                                  @Nullable String hiveDatabase) {
    return new NamespaceMeta.Builder()
      .setName(namespaceId)
      .setDescription("Namespace for authorization test")
      .setPrincipal(principal)
      .setGroupName(groupName)
      .setKeytabURI(keytabURI)
      .setRootDirectory(rootDirectory)
      .setHBaseNamespace(hbaseNamespace)
      .setHiveDatabase(hiveDatabase)
      .build();
  }
}
