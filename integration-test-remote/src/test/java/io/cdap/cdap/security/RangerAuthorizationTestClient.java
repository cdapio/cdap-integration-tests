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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class RangerAuthorizationTestClient implements AuthorizationTestClient {
  private final RangerAuthorizationClient authorizer;

  public RangerAuthorizationTestClient(RangerAuthorizationClient authorizer) {
    this.authorizer = authorizer;
  }

  @Override
  public void grant(String principal, EntityId entityId, Permission permission) throws Exception {
    grant(principal, entityId, permission, null);
  }

  @Override
  public void grant(String principal, EntityId entityId, Permission permission, @Nullable String groupName) {
    authorizer.grant(Authorizable.fromEntityId(entityId),
                     groupName == null ? new Principal(principal, Principal.PrincipalType.USER) :
                       new Principal(principal, Principal.PrincipalType.GROUP),
                     Collections.singleton(permission));
  }

  @Override
  public void wildCardGrant(String principal, Authorizable authorizable, Permission permission) {
    authorizer.grant(authorizable, new Principal(principal, Principal.PrincipalType.USER),
                     Collections.singleton(permission));
  }

  @Override
  public void revoke(String principal, EntityId entityId, Permission permission) {
    wildCardRevoke(principal, Authorizable.fromEntityId(entityId), permission);
  }

  @Override
  public void wildCardRevoke(String principal, Authorizable authorizable, Permission permission) {
    authorizer.revoke(authorizable, new Principal(principal, Principal.PrincipalType.USER),
                      Collections.singleton(permission));
  }

  @Override
  public void revokeAll(String principal) throws Exception {
    authorizer.revokeAll(principal);
  }

  @Override
  public void waitForAuthzCacheTimeout() throws Exception {
    // this is to make sure the cache times out in both master and remote side
    TimeUnit.SECONDS.sleep(10);
  }
}
