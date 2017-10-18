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

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.Authorizer;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Authorization test client used for sentry
 */
public class SentryAuthorizationTestClient implements AuthorizationTestClient {
  private final Authorizer authorizer;
  private final int cacheTimeout;

  public SentryAuthorizationTestClient(Authorizer authorizer, int cacheTimeout) {
    this.authorizer = authorizer;
    this.cacheTimeout = cacheTimeout;
  }

  @Override
  public void grant(String principal, EntityId entityId, Action action) throws Exception {
    grant(principal, entityId, action, null);
  }

  @Override
  public void grant(String principal, EntityId entityId, Action action, @Nullable String groupName) throws Exception {
    // grant to role and add to group
    authorizer.grant(Authorizable.fromEntityId(entityId), new Role(principal), EnumSet.of(action));
    authorizer.addRoleToPrincipal(
      new Role(principal), groupName == null ? new Principal(principal, Principal.PrincipalType.GROUP) :
        new Principal(groupName, Principal.PrincipalType.GROUP));
  }

  @Override
  public void wildCardGrant(String principal, Authorizable authorizable, Action action) throws Exception {
    authorizer.grant(authorizable, new Principal(principal, Principal.PrincipalType.ROLE), EnumSet.of(action));
    authorizer.addRoleToPrincipal(new Role(principal),
                                  new Principal(principal, Principal.PrincipalType.GROUP));
  }

  @Override
  public void revoke(String principal, EntityId entityId, Action action) throws Exception {
    wildCardRevoke(principal, Authorizable.fromEntityId(entityId), action);
  }

  @Override
  public void wildCardRevoke(String principal, Authorizable authorizable, Action action) throws Exception {
    authorizer.revoke(authorizable, new Role(principal), EnumSet.of(action));
  }

  @Override
  public void revokeAll(String principal) throws Exception {
    authorizer.dropRole(new Role(principal));
  }

  @Override
  public void waitForAuthzCacheTimeout() throws Exception {
    // sleep for the cache timeout
    TimeUnit.SECONDS.sleep(cacheTimeout);
  }

  public void createRole(Role role) throws Exception {
    authorizer.createRole(role);
  }
}
