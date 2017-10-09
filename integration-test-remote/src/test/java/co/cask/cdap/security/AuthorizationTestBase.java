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
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  // system admin user will generally have admin privilege on the entities in the test, and thus is generally used to
  // create namespace and other entities. Other users can also be used to create entities.
  protected static final String ADMIN_USER = "systemadmin";
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String VERSION = "1.0.0";
  protected static final String NO_ACCESS_MSG = "does not have privileges to access entity";
  protected static final String NO_PRIVILEGE_MESG = "is not authorized to perform action";
  protected static final String NOT_VISIBLE_MSG = "since the principal does not have any privilege on this namespace" +
    " or any entity in this namespace";
  protected static final String INSTANCE_NAME = "cdap";

  // cdapitn user will be running the tests, thus will have all wildcard privileges to be able to clean up entities
  // after each test
  private static final String ITN_ADMIN = "cdapitn";

  protected AuthorizationClient authorizationClient;

  // General test namespace
  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  // this is the cache time out for the authorizer
  protected int cacheTimeout;

  @Override
  public void setUp() throws Exception {
    ClientConfig systemConfig = getClientConfig(fetchAccessToken(ITN_ADMIN, ITN_ADMIN));
    RESTClient systemClient = new RESTClient(systemConfig);
    // this client is loggin as cdapitn, and cdapitn should be the sentry admin
    authorizationClient = new AuthorizationClient(systemConfig, systemClient);
    createAllUserRoles();
    grant(ITN_ADMIN, NamespaceId.DEFAULT, Action.ADMIN);
    grant(INSTANCE_NAME, NamespaceId.DEFAULT, Action.ADMIN);
    grantAlltoItnAdmin();
    waitForAuthzCacheTimeout();
    super.setUp();
  }

  @Before
  public void setup() throws Exception {
    Map<String, ConfigEntry> configs = this.getMetaClient().getCDAPConfig();
    ConfigEntry configEntry = configs.get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
    // cache time out is the sum of cache timeout on master side + cache timeout on remote side + buffer
    this.cacheTimeout = Integer.valueOf(configs.get("security.authorization.cache.ttl.secs").getValue()) +
      Integer.valueOf(configs.get("security.authorization.extension.config.cache.ttl.secs").getValue()) + 5;
  }

  @Override
  public void tearDown() throws Exception {
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    revoke(ADMIN_USER);
    revoke(ALICE);
    revoke(BOB);
    revoke(CAROL);
    revoke(EVE);
    revoke(INSTANCE_NAME);
    revoke(ITN_ADMIN);
    waitForAuthzCacheTimeout();
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

  /**
   * Grants action privilege to user on entityId.
   *
   * @param principal The principal we want to grant privilege to.
   * @param entityId The entity we want to grant privilege on.
   * @param action The privilege we want to grant.
   */
  protected void grant(String principal, EntityId entityId, Action action) throws Exception {
    grant(principal, entityId, action, null);
  }

  protected void grant(String principal, EntityId entityId, Action action,
                       @Nullable String groupName) throws Exception {
    // grant to role and add to group
    authorizationClient.grant(entityId, new Role(principal), EnumSet.of(action));
    authorizationClient.addRoleToPrincipal(
      new Role(principal), groupName == null ? new Principal(principal, Principal.PrincipalType.GROUP) :
        new Principal(groupName, Principal.PrincipalType.GROUP));
  }

  protected void wildCardGrant(String principal, co.cask.cdap.proto.security.Authorizable authorizable,
                               Action action) throws Exception {
    authorizationClient.grant(authorizable, new Principal(principal, Principal.PrincipalType.ROLE), EnumSet.of(action));
    authorizationClient.addRoleToPrincipal(new Role(principal),
                                           new Principal(principal, Principal.PrincipalType.GROUP));
  }

  protected void wildCardRevoke(String principal, co.cask.cdap.proto.security.Authorizable authorizable,
                                Action action) throws Exception {
    authorizationClient.revoke(authorizable, new Role(principal), EnumSet.of(action));
  }

  protected void revoke(String principal, EntityId entityId, Action action) throws Exception {
    wildCardRevoke(principal, Authorizable.fromEntityId(entityId), action);
  }

  /**
   * Revokes all privileges from the principal.
   *
   * @param principal The principal we want to revoke privilege from.
   */
  protected void revoke(String principal) throws Exception {
    authorizationClient.dropRole(new Role(principal));
  }

  protected void waitForAuthzCacheTimeout() throws Exception {
    // sleep for the cache timeout
    TimeUnit.SECONDS.sleep(cacheTimeout);
  }

  private void createAllUserRoles() throws Exception {
    authorizationClient.createRole(new Role(ADMIN_USER));
    authorizationClient.createRole(new Role(ALICE));
    authorizationClient.createRole(new Role(BOB));
    authorizationClient.createRole(new Role(CAROL));
    authorizationClient.createRole(new Role(EVE));
    authorizationClient.createRole(new Role(INSTANCE_NAME));
    authorizationClient.createRole(new Role(ITN_ADMIN));
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

  private void grantAlltoItnAdmin() throws Exception {
    Set<EntityType> entityTypes = ImmutableSet.<EntityType>builder()
      .add(EntityType.NAMESPACE, EntityType.APPLICATION, EntityType.PROGRAM, EntityType.ARTIFACT,
           EntityType.DATASET, EntityType.STREAM, EntityType.DATASET_MODULE, EntityType.DATASET_TYPE,
           EntityType.KERBEROSPRINCIPAL).build();
    for (EntityType entityType : entityTypes) {
      String authorizable = getWildCardString(entityType);
      wildCardGrant(ITN_ADMIN, co.cask.cdap.proto.security.Authorizable.fromString(authorizable), Action.ADMIN);
    }
  }

  private String getWildCardString(EntityType entityType) throws Exception {
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

  protected void setUpPrivileges(String user, Map<EntityId, Set<Action>> neededPrivileges) throws Exception {
    for (Map.Entry<EntityId, Set<Action>> privilege : neededPrivileges.entrySet()) {
      for (Action action : privilege.getValue()) {
        grant(user, privilege.getKey(), action);
      }
    }
  }
}
