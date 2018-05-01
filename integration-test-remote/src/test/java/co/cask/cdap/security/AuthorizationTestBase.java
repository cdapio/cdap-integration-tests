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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
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
  protected static final String CDAP_USER = "cdap";

  // cdapitn user will be running the tests, thus will have all wildcard privileges to be able to clean up entities
  // after each test
  private static final String ITN_ADMIN = "cdapitn";

  AuthorizationTestClient authorizationTestClient;

  // General test namespace
  NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  // this is the cache time out for the authorizer
  int cacheTimeout;

  @Override
  public void setUp() throws Exception {
    verifyAndSetUpAuthz();
    super.setUp();
  }

  private void verifyAndSetUpAuthz() throws Exception {
    ClientConfig systemConfig = getClientConfig(fetchAccessToken(ITN_ADMIN, ITN_ADMIN));
    RESTClient systemClient = new RESTClient(systemConfig);
    Map<String, ConfigEntry> configs = new MetaClient(systemConfig, systemClient).getCDAPConfig();
    ConfigEntry configEntry = configs.get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
    // cache time out is the sum of cache timeout on master side + cache timeout on remote side + buffer
    this.cacheTimeout = Integer.valueOf(configs.get("security.authorization.cache.ttl.secs").getValue()) +
      Integer.valueOf(configs.get("security.authorization.extension.config.cache.ttl.secs").getValue()) + 5;

    // this client is loggin as cdapitn, and cdapitn should be the sentry admin
    AuthorizationClient authorizationClient = new AuthorizationClient(systemConfig, systemClient);

    boolean isSentry = configs.get("security.authorization.extension.config.sentry.site.url") != null;
    if (isSentry) {
      authorizationTestClient = new SentryAuthorizationTestClient(authorizationClient, cacheTimeout);;
      createAllUserRoles(authorizationClient);
    } else {
      authorizationTestClient =
        new RangerAuthorizationTestClient(new RangerAuthorizationClient(authorizationClient, getInstanceURI()));
    }
    authorizationTestClient.grant(ITN_ADMIN, NamespaceId.DEFAULT, Action.ADMIN);
    authorizationTestClient.grant(CDAP_USER, NamespaceId.DEFAULT, Action.ADMIN);
    grantAlltoItnAdmin();
    authorizationTestClient.waitForAuthzCacheTimeout();
  }

  @Override
  public void tearDown() throws Exception {
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    authorizationTestClient.revokeAll(ADMIN_USER);
    authorizationTestClient.revokeAll(ALICE);
    authorizationTestClient.revokeAll(BOB);
    authorizationTestClient.revokeAll(CAROL);
    authorizationTestClient.revokeAll(EVE);
    authorizationTestClient.revokeAll(CDAP_USER);
    authorizationTestClient.revokeAll(ITN_ADMIN);
    authorizationTestClient.waitForAuthzCacheTimeout();
  }

  NamespaceId createAndRegisterNamespace(NamespaceMeta namespaceMeta, ClientConfig config,
                                         RESTClient client) throws Exception {
    try {
      new NamespaceClient(config, client).create(namespaceMeta);
    } finally {
      registerForDeletion(namespaceMeta.getNamespaceId());
    }
    return namespaceMeta.getNamespaceId();
  }

  private void createAllUserRoles(AuthorizationClient authorizationClient) throws Exception {
    authorizationClient.createRole(new Role(ADMIN_USER));
    authorizationClient.createRole(new Role(ALICE));
    authorizationClient.createRole(new Role(BOB));
    authorizationClient.createRole(new Role(CAROL));
    authorizationClient.createRole(new Role(EVE));
    authorizationClient.createRole(new Role(CDAP_USER));
    authorizationClient.createRole(new Role(ITN_ADMIN));
  }

  NamespaceMeta getNamespaceMeta(NamespaceId namespaceId, @Nullable String principal,
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
      authorizationTestClient.wildCardGrant(ITN_ADMIN, Authorizable.fromString(authorizable), Action.ADMIN);
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
        authorizationTestClient.grant(user, privilege.getKey(), action);
      }
    }
  }

  void verifyStreamReadWritePrivilege(StreamClient streamClient, StreamId streamId,
                                              Set<Action> allowedPrivileges) throws Exception {
    boolean hasRead = allowedPrivileges.contains(Action.READ);
    boolean hasWrite = allowedPrivileges.contains(Action.WRITE);
    try {
      streamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, new ArrayList<StreamEvent>());
      if (!hasRead) {
        Assert.fail("Stream read should fail since user does not have read privilege");
      }
    } catch (UnauthorizedException ex) {
      if (hasRead) {
        Assert.fail("Stream read should be successful since user has read privilege");
      }
      // otherwise it is expected
    }

    try {
      streamClient.sendEvent(streamId, "integration test message");
      if (!hasWrite) {
        Assert.fail("Stream write should fail since user does not have write privilege");
      }
    } catch (UnauthorizedException ex) {
      if (hasWrite) {
        Assert.fail("Stream write should be successful since user has write privilege");
      }
      // otherwise it is expected
    }
  }
}
