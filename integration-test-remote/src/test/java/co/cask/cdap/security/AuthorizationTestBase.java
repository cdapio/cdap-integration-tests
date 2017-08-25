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
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.authorization.sentry.model.Application;
import co.cask.cdap.security.authorization.sentry.model.Artifact;
import co.cask.cdap.security.authorization.sentry.model.Authorizable;
import co.cask.cdap.security.authorization.sentry.model.Dataset;
import co.cask.cdap.security.authorization.sentry.model.DatasetModule;
import co.cask.cdap.security.authorization.sentry.model.DatasetType;
import co.cask.cdap.security.authorization.sentry.model.Instance;
import co.cask.cdap.security.authorization.sentry.model.Namespace;
import co.cask.cdap.security.authorization.sentry.model.Program;
import co.cask.cdap.security.authorization.sentry.model.SecureKey;
import co.cask.cdap.security.authorization.sentry.model.Stream;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Authorization test base for all authorization tests
 */
public abstract class AuthorizationTestBase extends AudiTestBase {
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

  private static final String COMPONENT = "cdap";

  // TODO: Remove this when we migrate to wildcard privilege
  protected SentryGenericServiceClient sentryClient;
  protected AuthorizationClient authorizationClient;

  // General test namespace
  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  @Override
  public void setUp() throws Exception {
    sentryClient = SentryGenericServiceClientFactory.create(getSentryConfig());
    // TODO: remove this once caching in sentry is fixed
    ClientConfig systemConfig = getClientConfig(fetchAccessToken(ITN_ADMIN, ITN_ADMIN));
    RESTClient systemClient = new RESTClient(systemConfig);
    authorizationClient = new AuthorizationClient(systemConfig, systemClient);
   // userGrant(ITN_ADMIN, NamespaceId.DEFAULT, Action.ADMIN);
    grantAllWildCardPolicies();
    invalidateCache();
    super.setUp();
  }

  @Before
  public void setup() throws Exception {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

  @Override
  public void tearDown() throws Exception {
    // we have to grant ADMIN privileges to all clean up entites such that these entities can be deleted
//    for (EntityId entityId : cleanUpEntities) {
//      userGrant(ITN_ADMIN, entityId, Action.ADMIN);
//    }
//    userGrant(ITN_ADMIN, testNamespace.getNamespaceId(), Action.ADMIN);
//    userGrant(ITN_ADMIN, NamespaceId.DEFAULT, Action.ADMIN);
   // invalidateCache();
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    userRevoke(ADMIN_USER);
    userRevoke(ALICE);
    userRevoke(BOB);
    userRevoke(CAROL);
    userRevoke(EVE);
    userRevoke(INSTANCE_NAME);
    invalidateCache();
    sentryClient.close();
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
    // create role and add to group
    // TODO: use a different user as Sentry Admin (neither CDAP, nor an user used in our tests)
    sentryClient.createRoleIfNotExist(ITN_ADMIN, user, COMPONENT);
    sentryClient.addRoleToGroups(ITN_ADMIN, user, COMPONENT, Sets.newHashSet(user));

    List<TAuthorizable> authorizables = toTAuthorizable(authorizable);
    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT, INSTANCE_NAME, authorizables, action.name());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(ITN_ADMIN, user, COMPONENT, privilege);
  }

  protected void roleGrant(String role, EntityId entityId, Action action,
                           @Nullable String groupName) throws Exception {
    // create role and add to group
    // TODO: use a different user as Sentry Admin (neither CDAP, nor an user used in our tests)
    sentryClient.createRoleIfNotExist(ITN_ADMIN, role, COMPONENT);
    sentryClient.addRoleToGroups(ITN_ADMIN, role, COMPONENT,
                                 groupName == null ? Sets.newHashSet(role) : Sets.newHashSet(groupName));

    // create authorizable list
    List<TAuthorizable> authorizables = toTAuthorizable(entityId);
    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT, INSTANCE_NAME, authorizables, action.name());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(ITN_ADMIN, role, COMPONENT, privilege);
  }

  protected void invalidateCache() throws Exception {
    TimeUnit.SECONDS.sleep(7);
  }

  /**
   * Revokes all privileges from user. Deletes the user role through sentry.
   *
   * @param user The user we want to revoke privilege from.
   */
  protected void userRevoke(String user) throws Exception {
    roleRevoke(user, null);
  }

  protected void userRevoke(String user, EntityId entityId, Action action) throws Exception {
    // create authorizable list
    List<TAuthorizable> authorizables = toTAuthorizable(entityId);
    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT, INSTANCE_NAME, authorizables, action.name());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(ITN_ADMIN, user, COMPONENT, privilege);
  }

  protected void roleRevoke(String role, @Nullable String groupName) throws Exception {
    try {
      sentryClient.deleteRoleToGroups(ITN_ADMIN, role, COMPONENT,
                                      groupName == null ? Sets.newHashSet(role) : Sets.newHashSet(groupName));
    } catch (SentryNoSuchObjectException e) {
      // skip a role that hasn't been added to the user
    } finally {
      sentryClient.dropRoleIfExists(ITN_ADMIN, role, COMPONENT);
    }
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
      if (!entityType.equals(EntityType.PROGRAM)) {
        String authorizable = getWildCardString(entityType, null);
        wildCardGrant(ITN_ADMIN, co.cask.cdap.proto.security.Authorizable.fromString(authorizable), Action.ADMIN);
      } else {
        for (ProgramType programType : ProgramType.values()) {
          String authorizable = getWildCardString(entityType, programType);
          if (programType != ProgramType.CUSTOM_ACTION) {
            wildCardGrant(ITN_ADMIN, co.cask.cdap.proto.security.Authorizable.fromString(authorizable), Action.ADMIN);
          }
        }
      }
    }
  }

  private String getWildCardString(EntityType entityType, @Nullable ProgramType programType) throws Exception {
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
        return prefix + "*.*." + programType.toString() +".*";
      case KERBEROSPRINCIPAL:
        return prefix + "*";
      default:
        throw new IllegalArgumentException(String.format("The entity is of unknown type %s", entityType));
    }
  }

  private Configuration getSentryConfig() throws IOException, LoginException, URISyntaxException {

    String hostUri = super.getInstanceURI();
    String sentryRpcAddr = new URI(hostUri).getHost();
    String sentryPrincipal = "sentry/" + sentryRpcAddr + "@CONTINUUITY.NET";
    String sentryRpcPort = "8038";

    Configuration conf = new Configuration(false);
    conf.clear();
    conf.set(ServiceConstants.ServerConfig.SECURITY_MODE, ServiceConstants.ServerConfig.SECURITY_MODE_KERBEROS);
    conf.set(ServiceConstants.ServerConfig.PRINCIPAL, sentryPrincipal);
    conf.set(ServiceConstants.ClientConfig.SERVER_RPC_ADDRESS, sentryRpcAddr);
    conf.set(ServiceConstants.ClientConfig.SERVER_RPC_PORT, sentryRpcPort);

    // Log in to Kerberos
    UserGroupInformation.setConfiguration(conf);
    LoginContext lc = kinit();
    UserGroupInformation.loginUserFromSubject(lc.getSubject());
    return conf;
  }

  private static LoginContext kinit() throws LoginException {
    LoginContext lc = new LoginContext(BasicAuthorizationTest.class.getSimpleName(), new CallbackHandler() {
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback c : callbacks) {
          if (c instanceof NameCallback) {
            ((NameCallback) c).setName(ITN_ADMIN);
          }
          if (c instanceof PasswordCallback) {
            ((PasswordCallback) c).setPassword(ITN_ADMIN.toCharArray());
          }
        }
      }
    });
    lc.login();
    return lc;
  }

  private List<TAuthorizable> toTAuthorizable(EntityId entityId) {
    List<org.apache.sentry.core.common.Authorizable> authorizables = toSentryAuthorizables(entityId);
    List<TAuthorizable> tAuthorizables = new ArrayList<>();
    for (org.apache.sentry.core.common.Authorizable authorizable : authorizables) {
      tAuthorizables.add(new TAuthorizable(authorizable.getTypeName(), authorizable.getName()));
    }
    return tAuthorizables;
  }

  @VisibleForTesting
  List<org.apache.sentry.core.common.Authorizable> toSentryAuthorizables(final EntityId entityId) {
    List<org.apache.sentry.core.common.Authorizable> authorizables = new LinkedList<>();
    toAuthorizables(entityId, authorizables);
    return authorizables;
  }

  private void toAuthorizables(EntityId entityId, List<? super Authorizable> authorizables) {
    EntityType entityType = entityId.getEntityType();
    switch (entityType) {
      case INSTANCE:
        authorizables.add(new Instance(((InstanceId) entityId).getInstance()));
        break;
      case NAMESPACE:
        toAuthorizables(new InstanceId(INSTANCE_NAME), authorizables);
        authorizables.add(new Namespace(((NamespaceId) entityId).getNamespace()));
        break;
      case ARTIFACT:
        ArtifactId artifactId = (ArtifactId) entityId;
        toAuthorizables(artifactId.getParent(), authorizables);
        authorizables.add(new Artifact(artifactId.getArtifact()));
        break;
      case APPLICATION:
        ApplicationId applicationId = (ApplicationId) entityId;
        toAuthorizables(applicationId.getParent(), authorizables);
        authorizables.add(new Application(applicationId.getApplication()));
        break;
      case DATASET:
        DatasetId dataset = (DatasetId) entityId;
        toAuthorizables(dataset.getParent(), authorizables);
        authorizables.add(new Dataset(dataset.getDataset()));
        break;
      case DATASET_MODULE:
        DatasetModuleId datasetModuleId = (DatasetModuleId) entityId;
        toAuthorizables(datasetModuleId.getParent(), authorizables);
        authorizables.add(new DatasetModule(datasetModuleId.getModule()));
        break;
      case DATASET_TYPE:
        DatasetTypeId datasetTypeId = (DatasetTypeId) entityId;
        toAuthorizables(datasetTypeId.getParent(), authorizables);
        authorizables.add(new DatasetType(datasetTypeId.getType()));
        break;
      case STREAM:
        StreamId streamId = (StreamId) entityId;
        toAuthorizables(streamId.getParent(), authorizables);
        authorizables.add(new Stream((streamId).getStream()));
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        toAuthorizables(programId.getParent(), authorizables);
        authorizables.add(new Program(programId.getType(), programId.getProgram()));
        break;
      case SECUREKEY:
        SecureKeyId secureKeyId = (SecureKeyId) entityId;
        toAuthorizables(secureKeyId.getParent(), authorizables);
        authorizables.add(new SecureKey(secureKeyId.getName()));
        break;
      case KERBEROSPRINCIPAL:
        KerberosPrincipalId principalId = (KerberosPrincipalId) entityId;
        toAuthorizables(new InstanceId(INSTANCE_NAME), authorizables);
        authorizables.add(new co.cask.cdap.security.authorization.sentry.model.Principal(principalId.getPrincipal()));
        break;
      default:
        throw new IllegalArgumentException(String.format("The entity %s is of unknown type %s", entityId, entityType));
    }
  }

  // TODO: Remove this when we migrate to wildcard privilege
  protected void setUpPrivilegeAndRegisterForDeletion(String user,
                                                      Map<EntityId, Set<Action>> neededPrivileges) throws Exception {
    for (Map.Entry<EntityId, Set<Action>> privilege : neededPrivileges.entrySet()) {
      for (Action action : privilege.getValue()) {
        userGrant(user, privilege.getKey(), action);
      }
    }
  }

  @VisibleForTesting
  private List<org.apache.sentry.core.common.Authorizable>
  toSentryAuthorizables(final co.cask.cdap.proto.security.Authorizable authorizable) {
    List<org.apache.sentry.core.common.Authorizable> authorizables = new LinkedList<>();
    toSentryAuthorizables(authorizable.getEntityType(), authorizable, authorizables);
    return authorizables;
  }

  private List<TAuthorizable> toTAuthorizable(co.cask.cdap.proto.security.Authorizable authorizable) {
    List<org.apache.sentry.core.common.Authorizable> sentryAuthorizables = toSentryAuthorizables(authorizable);
    List<TAuthorizable> tAuthorizables = new ArrayList<>();
    for (org.apache.sentry.core.common.Authorizable authz : sentryAuthorizables) {
      tAuthorizables.add(new TAuthorizable(authz.getTypeName(), authz.getName()));
    }
    return tAuthorizables;
  }

  private void toSentryAuthorizables(EntityType curType, co.cask.cdap.proto.security.Authorizable authorizable,
                                     List<? super Authorizable> sentryAuthorizables) {
    switch (curType) {
      case INSTANCE:
        sentryAuthorizables.add(new Instance(authorizable.getEntityParts().get(EntityType.INSTANCE)));
        break;
      case NAMESPACE:
        sentryAuthorizables.add(new Instance(INSTANCE_NAME));
        sentryAuthorizables.add(new Namespace(authorizable.getEntityParts().get(curType)));
        break;
      case ARTIFACT:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new Artifact(authorizable.getEntityParts().get(curType)));
        break;
      case APPLICATION:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new Application(authorizable.getEntityParts().get(curType)));
        break;
      case DATASET:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new Dataset(authorizable.getEntityParts().get(curType)));
        break;
      case DATASET_MODULE:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new DatasetModule(authorizable.getEntityParts().get(curType)));
        break;
      case DATASET_TYPE:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new DatasetType(authorizable.getEntityParts().get(curType)));
        break;
      case STREAM:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new Stream(authorizable.getEntityParts().get(curType)));
        break;
      case PROGRAM:
        toSentryAuthorizables(EntityType.APPLICATION, authorizable, sentryAuthorizables);
        String[] programDetails = authorizable.getEntityParts().get(curType).split("\\.");
        sentryAuthorizables.add(new Program(ProgramType.valueOf(programDetails[0].toUpperCase()), programDetails[1]));
        break;
      case SECUREKEY:
        toSentryAuthorizables(EntityType.NAMESPACE, authorizable, sentryAuthorizables);
        sentryAuthorizables.add(new SecureKey(authorizable.getEntityParts().get(curType)));
        break;
      case KERBEROSPRINCIPAL:
        sentryAuthorizables.add(new Instance(INSTANCE_NAME));
        sentryAuthorizables.add(new co.cask.cdap.security.authorization.sentry.model.Principal(
          authorizable.getEntityParts().get(curType)));
        break;
      default:
        throw new IllegalArgumentException(String.format("The entity %s is of unknown type %s",
                                                         authorizable.getEntityParts(), authorizable.getEntityType()));
    }
  }
}
