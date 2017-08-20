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
  protected static final String ADMIN_USER = "cdapitn";
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String VERSION = "1.0.0";
  protected static final String NO_PRIVILEGE_MSG = "does not have privileges to access entity";

  private static final String COMPONENT = "cdap";
  private static final String INSTANCE_NAME = "cdap";
  private static final Role DUMMY_ROLE = new Role("dummy");

  private SentryGenericServiceClient sentryClient;
  private AuthorizationClient authorizationClient;

  // General test namespace
  protected NamespaceMeta testNamespace = getNamespaceMeta(new NamespaceId("authorization"), null, null,
                                                           null, null, null, null);

  @Before
  public void setup() throws Exception {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
    sentryClient = SentryGenericServiceClientFactory.create(getSentryConfig());
    // TODO: remove this once caching in sentry is fixed
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    authorizationClient = new AuthorizationClient(adminConfig, adminClient);
  }

  @Override
  public void tearDown() throws Exception {
    userGrant(ADMIN_USER, testNamespace.getNamespaceId(), Action.ADMIN);
    // teardown in parent deletes all entities
    super.tearDown();
    // reset the test by revoking privileges from all users.
    userRevoke(ADMIN_USER);
    userRevoke(ALICE);
    userRevoke(BOB);
    userRevoke(CAROL);
    userRevoke(EVE);
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
   *
   * @param user The user we want to grant privilege to.
   * @param entityId The entity we want to grant privilege on.
   * @param action The privilege we want to grant.
   */
  protected void userGrant(String user, EntityId entityId, Action action) throws Exception {
    // create role and add to group
    // TODO: use a different user as Sentry Admin (neither CDAP, nor an user used in our tests)
    sentryClient.createRoleIfNotExist(ADMIN_USER, user, COMPONENT);
    sentryClient.addRoleToGroups(ADMIN_USER, user, COMPONENT, Sets.newHashSet(user));

    // create authorizable list
    List<TAuthorizable> authorizables = toTAuthorizable(entityId);
    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT, INSTANCE_NAME, authorizables, action.name());
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(ADMIN_USER, user, COMPONENT, privilege);
    // TODO: Hack to invalidate cache in sentry authorizer. Remove once cache problem is solved.
    authorizationClient.dropRole(DUMMY_ROLE);
  }

  /**
   * Revokes all privileges from user. Deletes the user role through sentry.
   *
   * @param user The user we want to revoke privilege from.
   */
  protected void userRevoke(String user) throws Exception {
    try {
      sentryClient.deleteRoleToGroups(ADMIN_USER, user, COMPONENT, Sets.newHashSet(user));
    } catch (SentryNoSuchObjectException e) {
      // skip a role that hasn't been added to the user
    } finally {
      sentryClient.dropRoleIfExists(ADMIN_USER, user, COMPONENT);
      // TODO: Hack to invalidate cache in sentry authorizer. Remove once cache problem is solved.
      authorizationClient.dropRole(DUMMY_ROLE);
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
            ((NameCallback) c).setName(ADMIN_USER);
          }
          if (c instanceof PasswordCallback) {
            ((PasswordCallback) c).setPassword(ADMIN_USER.toCharArray());
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

  void toAuthorizables(EntityId entityId, List<? super Authorizable> authorizables) {
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
        authorizables.add(new Artifact(artifactId.getArtifact(), artifactId.getVersion()));
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
}
