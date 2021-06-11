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

import javax.annotation.Nullable;

/**
 * Client used by integration tests to manage the privileges.
 */
public interface AuthorizationTestClient {

  /**
   * Grant action privilege to a principal on an entityId.
   */
  void grant(String principal, EntityId entityId, Permission permission) throws Exception;

  /**
   * Grant action privilege to a principal on an entityId and add it to group
   */
  void grant(String principal, EntityId entityId, Permission permission, @Nullable String groupName) throws Exception;

  /**
   * Grant a wildcard privilege to a principal on an authorizable.
   */
  void wildCardGrant(String principal, Authorizable authorizable, Permission permission) throws Exception;

  /**
   * Revoke action privilege from a principal on an entityId.
   */
  void revoke(String principal, EntityId entityId, Permission permission) throws Exception;

  /**
   * Revoke a wildcard privilege from a principal on an authorizable.
   */
  void wildCardRevoke(String principal, Authorizable authorizable, Permission permission) throws Exception;

  /**
   * Revoke all privileges from a principal.
   */
  void revokeAll(String principal) throws Exception;

  /**
   * Wait for the cache time out.
   */
  void waitForAuthzCacheTimeout() throws Exception;
}
