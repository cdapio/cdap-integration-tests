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

import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Authorization test base for all authorization tests
 */
public abstract class AuthorizationTestBase extends AudiTestBase {
  protected static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  protected static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  protected static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  protected static final Principal CAROL = new Principal("carol", Principal.PrincipalType.USER);
  protected static final Principal EVE = new Principal("eve", Principal.PrincipalType.USER);
  protected static final Principal ADMIN_USER = new Principal("systemadmin", Principal.PrincipalType.USER);
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String VERSION = "1.0.0";
  protected static final String NO_ACCESS_MSG = "does not have privileges to access entity";
  protected static final String NO_PRIVILEGE_MESG = "is not authorized to perform action";
  protected static final String INSTANCE_NAME = "cdap";
  protected static final Principal CDAP_USER = new Principal("cdap", Principal.PrincipalType.USER);
  ;
  protected static final Principal ITN_ADMIN = new Principal("cdapitn", Principal.PrincipalType.USER);

  // this client is loggin as cdapitn, and cdapitn should be the sentry admin
//  protected AuthorizationClient authorizationClient;
  protected Authorizer authorizer;


  // General test namespace
  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  Integer authorizerCacheTimeout;

  @Override
  public void setUp() throws Exception {
    setUpSentry();
  }

  private void setUpSentry() throws Exception {
    ClientConfig systemConfig = getClientConfig(fetchAccessToken(ITN_ADMIN.getName(), ITN_ADMIN.getName()));
    RESTClient systemClient = new RESTClient(systemConfig);
    authorizer = new RBACAuthorizationClient(new AuthorizationClient(systemConfig, systemClient), 10);
    createAllUserRoles();
    authorizer.grant(NamespaceId.DEFAULT, ITN_ADMIN, EnumSet.of(Action.ADMIN));
    grantAllWildCardPolicies();
    ((RBACAuthorizationClient) authorizer).invalidateCache();
    super.setUp();
    authorizerCacheTimeout = Integer.valueOf(this.getMetaClient().getCDAPConfig().get("security.authorization" +
                                                                                        ".cache.ttl.secs").getValue());
  }

  @Override
  public void tearDown() throws Exception {
    tearDownSentry();
  }

  private void tearDownSentry() throws Exception {
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    authorizer.dropRole(new Role(ADMIN_USER.getName()));
    authorizer.dropRole(new Role(ALICE.getName()));
    authorizer.dropRole(new Role(BOB.getName()));
    authorizer.dropRole(new Role(CAROL.getName()));
    authorizer.dropRole(new Role(EVE.getName()));
    authorizer.dropRole(new Role(CDAP_USER.getName()));
    authorizer.dropRole(new Role(ITN_ADMIN.getName()));
    ((RBACAuthorizationClient) authorizer).invalidateCache();
  }

  @Before
  public void setup() throws Exception {
    Map<String, ConfigEntry> configs = this.getMetaClient().getCDAPConfig();
    ConfigEntry configEntry = configs.get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
//    this.cacheTimeout = Integer.valueOf(configs.get("security.authorization.cache.ttl.secs").getValue());
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

  String getWildCardString(EntityType entityType) throws Exception {
    String prefix = entityType.toString().toLowerCase() + ":";
    switch (entityType) {
      case NAMESPACE:
        return prefix + "*";
      case ARTIFACT:
        return prefix + "*.*";
      case APPLICATION:
        return prefix + "*.*";
      case DATASET:
        return prefix + "*.*";
      case DATASET_MODULE:
        return prefix + "*.*";
      case DATASET_TYPE:
        return prefix + "*.*";
      case STREAM:
        return prefix + "*.*";
      case PROGRAM:
        return prefix + "*.*.*";
      case KERBEROSPRINCIPAL:
        return prefix + "*";
      default:
        throw new IllegalArgumentException(String.format("The entity is of unknown type %s", entityType));
    }
  }

  void grantAllWildCardPolicies() throws Exception {
    Set<EntityType> entityTypes = ImmutableSet.<EntityType>builder()
      .add(EntityType.NAMESPACE, EntityType.APPLICATION, EntityType.PROGRAM, EntityType.ARTIFACT,
           EntityType.DATASET, EntityType.STREAM, EntityType.DATASET_MODULE, EntityType.DATASET_TYPE,
           EntityType.KERBEROSPRINCIPAL).build();
    for (EntityType entityType : entityTypes) {
      String authorizable = getWildCardString(entityType);
      authorizer.grant(co.cask.cdap.proto.security.Authorizable.fromString(authorizable), ITN_ADMIN,
                       EnumSet.of(Action.ADMIN));
    }
  }

  void createAllUserRoles() throws Exception {
    authorizer.createRole(new Role(CDAP_USER.getName()));
    authorizer.createRole(new Role(ALICE.getName()));
    authorizer.createRole(new Role(BOB.getName()));
    authorizer.createRole(new Role(CAROL.getName()));
    authorizer.createRole(new Role(EVE.getName()));
    authorizer.createRole(new Role(ADMIN_USER.getName()));
    authorizer.createRole(new Role(ITN_ADMIN.getName()));
  }
}
