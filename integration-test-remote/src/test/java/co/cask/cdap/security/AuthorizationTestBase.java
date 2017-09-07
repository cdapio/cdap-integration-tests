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
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Authorization test base for all authorization tests
 */
public abstract class AuthorizationTestBase extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationTestBase.class);
  protected static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  protected static final String ALICE = "alice";
  protected static final String BOB = "bob";
  protected static final String CAROL = "carol";
  protected static final String EVE = "eve";
  protected static final String ADMIN_USER = "systemadmin";
  protected static final String ITN_ADMIN = "cdapitn";
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String VERSION = "1.0.0";
  protected static final String NO_ACCESS_MSG = "does not have privileges to access entity";
  protected static final String NO_PRIVILEGE_MESG = "is not authorized to perform actions";
  protected static final String INSTANCE_NAME = "cdap";

  // this client is using
  protected AuthorizationClient authorizationClient;

  // General test namespace
  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  // this is the cache time out for the authorizer
  private int cacheTimeout;

  @Override
  public void setUp() throws Exception {
    ClientConfig systemConfig = getClientConfig(fetchAccessToken(ITN_ADMIN, ITN_ADMIN));
    RESTClient systemClient = new RESTClient(systemConfig);
    authorizationClient = new AuthorizationClient(systemConfig, systemClient);
    createAllUserRoles();
    userGrant(ITN_ADMIN, NamespaceId.DEFAULT, Action.ADMIN);
    grantAllWildCardPolicies();
    invalidateCache();
    super.setUp();
  }

  @Before
  public void setup() throws Exception {
    Map<String, ConfigEntry> configs = this.getMetaClient().getCDAPConfig();
    ConfigEntry configEntry = configs.get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
    this.cacheTimeout = Integer.valueOf(configs.get("security.authorization.cache.ttl.secs").getValue());
  }

  @Override
  public void tearDown() throws Exception {
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    userRevoke(ADMIN_USER);
    userRevoke(ALICE);
    userRevoke(BOB);
    userRevoke(CAROL);
    userRevoke(EVE);
    userRevoke(INSTANCE_NAME);
    userRevoke(ITN_ADMIN);
    invalidateCache();
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
   * Grants action privilege to user on entityId. Creates a role for the user. Grant action privilege
   * on that role, and add the role to the group the user belongs to. All done through sentry.
   * @param user The user we want to grant privilege to.
   * @param entityId The entity we want to grant privilege on.
   * @param action The privilege we want to grant.
   */
  protected void userGrant(String user, EntityId entityId, Action action) throws Exception {
    roleGrant(user, entityId, action, null);
  }

  protected void wildCardGrant(String user, co.cask.cdap.proto.security.Authorizable authorizable,
                               Action action) throws Exception {
    authorizationClient.grant(authorizable, new Principal(user, Principal.PrincipalType.ROLE), EnumSet.of(action));
    authorizationClient.addRoleToPrincipal(new Role(user), new Principal(user, Principal.PrincipalType.GROUP));
  }

  protected void wildCardRevoke(String user, co.cask.cdap.proto.security.Authorizable authorizable,
                                Action action) throws Exception {
    authorizationClient.revoke(authorizable, new Role(user), EnumSet.of(action));
  }

  protected void roleGrant(String role, EntityId entityId, Action action,
                           @Nullable String groupName) throws Exception {
    // grant to role and add to group
    authorizationClient.grant(entityId, new Role(role), EnumSet.of(action));
    authorizationClient.addRoleToPrincipal(
      new Role(role), groupName == null ? new Principal(role, Principal.PrincipalType.GROUP) :
        new Principal(groupName, Principal.PrincipalType.GROUP));
  }

  protected void invalidateCache() throws Exception {
    // this is to make sure the cache times out in both master and remote side
    TimeUnit.SECONDS.sleep(2 * cacheTimeout + 5);
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

  /**
   * Revokes all privileges from user. Deletes the user role through sentry.
   *
   * @param user The user we want to revoke privilege from.
   */
  private void userRevoke(String user) throws Exception {
    authorizationClient.dropRole(new Role(user));
  }

  protected void userRevoke(String user, EntityId entityId, Action action) throws Exception {
    authorizationClient.revoke(entityId, new Principal(user, Principal.PrincipalType.ROLE), EnumSet.of(action));
  }

  protected void roleRevoke(String role, @Nullable String groupName) throws Exception {
    authorizationClient.dropRole(new Role(role));
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

  private void grantAllWildCardPolicies() throws Exception {
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

  protected void setUpPrivilegeAndRegisterForDeletion(String user,
                                                      Map<EntityId, Set<Action>> neededPrivileges) throws Exception {
    for (Map.Entry<EntityId, Set<Action>> privilege : neededPrivileges.entrySet()) {
      for (Action action : privilege.getValue()) {
        userGrant(user, privilege.getKey(), action);
      }
    }
  }
}
