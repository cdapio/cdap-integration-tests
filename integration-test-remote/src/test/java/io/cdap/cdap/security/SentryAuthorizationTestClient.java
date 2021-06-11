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

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AccessController;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Authorization test client used for sentry
 */
public class SentryAuthorizationTestClient implements AuthorizationTestClient {
  private final AccessController accessController;
  private final int cacheTimeout;

  public SentryAuthorizationTestClient(AccessController accessController, int cacheTimeout) {
    this.accessController = accessController;
    this.cacheTimeout = cacheTimeout;
  }

  @Override
  public void grant(String principal, EntityId entityId, Permission permission) throws Exception {
    grant(principal, entityId, permission, null);
  }

  @Override
  public void grant(String principal, EntityId entityId, Permission permission, @Nullable String groupName) {
    // grant to role and add to group
    accessController.grant(Authorizable.fromEntityId(entityId), new Role(principal), Collections.singleton(permission));
    accessController.addRoleToPrincipal(
      new Role(principal), groupName == null ? new Principal(principal, Principal.PrincipalType.GROUP) :
        new Principal(groupName, Principal.PrincipalType.GROUP));
  }

  @Override
  public void wildCardGrant(String principal, Authorizable authorizable, Permission permission) {
    accessController.grant(authorizable, new Principal(principal, Principal.PrincipalType.ROLE),
                           Collections.singleton(permission));
    accessController.addRoleToPrincipal(new Role(principal),
                                        new Principal(principal, Principal.PrincipalType.GROUP));
  }

  @Override
  public void revoke(String principal, EntityId entityId, Permission permission) throws Exception {
    wildCardRevoke(principal, Authorizable.fromEntityId(entityId), permission);
  }

  @Override
  public void wildCardRevoke(String principal, Authorizable authorizable, Permission permission) throws Exception {
    accessController.revoke(authorizable, new Role(principal), Collections.singleton(permission));
  }

  @Override
  public void revokeAll(String principal) throws Exception {
    accessController.dropRole(new Role(principal));
  }

  @Override
  public void waitForAuthzCacheTimeout() throws Exception {
    // sleep for the cache timeout
    TimeUnit.SECONDS.sleep(cacheTimeout);
  }

  public void createRole(Role role) throws Exception {
    accessController.createRole(role);
  }
}
