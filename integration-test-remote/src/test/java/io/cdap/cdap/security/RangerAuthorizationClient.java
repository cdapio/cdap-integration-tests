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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.client.AuthorizationClient;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.authorization.ranger.commons.RangerCommon;
import io.cdap.cdap.security.spi.AccessIOException;
import io.cdap.cdap.security.spi.authorization.AccessController;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An Authorization Client for Ranger which uses Ranger REST APIs for privilege management
 */
public class RangerAuthorizationClient implements AccessController {
  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  private enum UpdateType {
    GRANT,
    REVOKE
  }

  private final AuthorizationClient authorizationClient;
  private final String instaceURI;

  public RangerAuthorizationClient(AuthorizationClient authorizationClient, String instaceURI) {
    this.authorizationClient = authorizationClient;
    this.instaceURI = instaceURI;
  }

  @Override
  public void createRole(Role role) {
    authorizationClient.createRole(role);
  }

  @Override
  public void dropRole(Role role) {
    authorizationClient.dropRole(role);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) {
    authorizationClient.addRoleToPrincipal(role, principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) {
    authorizationClient.removeRoleFromPrincipal(role, principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) {
    return authorizationClient.listRoles(principal);
  }

  @Override
  public Set<Role> listAllRoles() {
    return authorizationClient.listAllRoles();
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
    authorizationClient.enforce(entity, principal, permissions);
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission) {
    authorizationClient.enforceOnParent(entityType, parentId, principal, permission);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> set, Principal principal) {
    return authorizationClient.isVisible(set, principal);
  }


  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> set)
    throws AccessException {

    try {
      List<RangerPolicy> existingPolicies = searchPolicy("policyName=" + authorizable.toString());
      if (existingPolicies != null && !existingPolicies.isEmpty()) {
        updatePolicyItems(existingPolicies, authorizable, principal, set, UpdateType.GRANT);
        return;
      }
      // create new policy
      String json = GSON.toJson(createRangerPolicy(authorizable, principal, set));
      HttpURLConnection conn = getRangerConnection("policy");
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      // its okay to hardcode the username and password here since in all our clusters we just use the dummy auth
      conn.setRequestProperty("Authorization", "Basic " +
        javax.xml.bind.DatatypeConverter.printBase64Binary("admin:admin".getBytes()));
      conn.setRequestProperty("charset", "utf-8");

      conn.setRequestProperty("Content-Length", Integer.toString(json.length()));

      try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
        wr.write(json.getBytes(StandardCharsets.UTF_8));
      }
      Assert.assertEquals(conn.getResponseMessage(), HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } catch (IOException e) {
      throw new AccessIOException(e);
    }

  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> set) {
    try {
      List<RangerPolicy> existingPolicies = searchPolicy("policyName=" + authorizable.toString());
      if (existingPolicies != null && !existingPolicies.isEmpty()) {
        updatePolicyItems(existingPolicies, authorizable, principal, set, UpdateType.REVOKE);
      }
    } catch (IOException e) {
      throw new AccessIOException(e);
    }
  }

  @Override
  public void revoke(Authorizable authorizable) {
    authorizationClient.revoke(authorizable);
  }

  // Note: This is bad design but all authorization tests are written without Principal and in compatible with CDAP's
  // Authorizer API
  public void revokeAll(String username) throws IOException {
    List<RangerPolicy> rangerPolicies = searchPolicy("user=" + username);
    for (RangerPolicy rangerPolicy : rangerPolicies) {
      deletePolicy(rangerPolicy.getId());
    }
  }

  private void deletePolicy(Long policyId) throws IOException {
    HttpURLConnection conn = getRangerConnection("policy/" + policyId);
    conn.setDoOutput(true);
    conn.setRequestMethod("DELETE");
    conn.setRequestProperty("Authorization", "Basic " +
      javax.xml.bind.DatatypeConverter.printBase64Binary("admin:admin".getBytes()));

    int responseCode = conn.getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_NO_CONTENT, responseCode);
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    return authorizationClient.listGrants(principal);
  }

  protected List<RangerPolicy> searchPolicy(String query) throws IOException {
    HttpURLConnection conn = getRangerConnection("service/cdapdev/policy?" + query);
    conn.setDoOutput(true);
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", "Basic " +
      javax.xml.bind.DatatypeConverter.printBase64Binary("admin:admin".getBytes()));

    int responseCode = conn.getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, responseCode);

    BufferedReader in = new BufferedReader(
      new InputStreamReader(conn.getInputStream()));
    String inputLine;
    StringBuilder response = new StringBuilder();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    Type listType = new TypeToken<ArrayList<RangerPolicy>>() {
    }.getType();
    return GSON.fromJson(response.toString(), listType);
  }

  private void updatePolicyItems(List<RangerPolicy> policies, Authorizable authorizable, Principal principal,
                                 Set<? extends Permission> permissions, UpdateType updateType) throws IOException {
    if (policies.size() != 1) {
      throw new RuntimeException("No or multiple existing policies found.");
    }
    Long policyId = policies.get(0).getId();
    List<RangerPolicy.RangerPolicyItem> updatedPolicyItems = updatePolicyItems(policies.get(0).getPolicyItems(),
                                                                               principal, permissions, updateType);
    String updatedPolicy = GSON.toJson(new RangerPolicy("cdapdev", authorizable.toString(), null, "",
                                                        policies.get(0).getResources(),
                                                        updatedPolicyItems, null));

    HttpURLConnection conn = getRangerConnection("policy/" + policyId);
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");
    // its okay to hardcode the username and password here since in all our clusters we just use the dummy auth
    conn.setRequestProperty("Authorization", "Basic " +
      javax.xml.bind.DatatypeConverter.printBase64Binary("admin:admin".getBytes()));
    conn.setRequestProperty("charset", "utf-8");

    conn.setRequestProperty("Content-Length", Integer.toString(updatedPolicy.length()));

    try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(updatedPolicy.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(conn.getResponseMessage(), HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  private List<RangerPolicy.RangerPolicyItem> updatePolicyItems(List<RangerPolicy.RangerPolicyItem> existing, Principal
    principal, Set<? extends Permission> permissions, UpdateType updateType) {
    List<RangerPolicy.RangerPolicyItem> result = new LinkedList<>();
    boolean updatedExistingPrivilege = false;
    for (RangerPolicy.RangerPolicyItem rangerPolicyItem : existing) {
      RangerPolicy.RangerPolicyItem updatedPolicy = rangerPolicyItem;
      HashSet<RangerPolicy.RangerPolicyItemAccess> existingAccesses = Sets.newHashSet(rangerPolicyItem.getAccesses());
      if (principal.getType() == Principal.PrincipalType.GROUP || principal.getType() == Principal.PrincipalType.USER) {
        if (rangerPolicyItem.getGroups().contains(principal.getName().toLowerCase()) ||
          rangerPolicyItem.getUsers().contains(principal.getName().toLowerCase())) {
          updatedExistingPrivilege = true;
          HashSet<RangerPolicy.RangerPolicyItemAccess> updatedPolices = Sets.newHashSet(getAccesses(permissions));
          if (updateType == UpdateType.GRANT) {
            existingAccesses.addAll(updatedPolices);
          } else {
            existingAccesses.removeAll(updatedPolices);
          }

          if (!existingAccesses.isEmpty()) {
            updatedPolicy = new RangerPolicy.RangerPolicyItem(
              new LinkedList<>(existingAccesses), rangerPolicyItem.getUsers(), rangerPolicyItem.getGroups(),
              rangerPolicyItem.getConditions(), rangerPolicyItem.getDelegateAdmin());
          }
        }
      }
      if (existingAccesses != null && !existingAccesses.isEmpty()) {
        result.add(updatedPolicy);
      }
    }
    if (!updatedExistingPrivilege && updateType == UpdateType.GRANT) {
      result.addAll(getPolicyItems(principal, permissions));
    }
    return result;
  }

  private HttpURLConnection getRangerConnection(String urlPart) throws IOException {
    URL url = new URL(instaceURI.replace("11015", "6080") + "/service/public/v2/api/" + urlPart);
    return (HttpURLConnection) url.openConnection();
  }

  private RangerPolicy createRangerPolicy(Authorizable authorizable, Principal principal,
                                          Set<? extends Permission> actions) {
    Map<String, RangerPolicy.RangerPolicyResource> resource = new LinkedHashMap<>();
    setRangerResource(authorizable, resource);
    return new RangerPolicy("cdapdev", authorizable.toString(), 0, "", resource,
                            getPolicyItems(principal, actions),
                            null);
  }

  private List<RangerPolicy.RangerPolicyItem> getPolicyItems(Principal principal,
                                                             Set<? extends Permission> permissions) {
    List<String> prin = ImmutableList.of(principal.getName());
    RangerPolicy.RangerPolicyItem rangerPolicyItem;
    if (principal.getType() == Principal.PrincipalType.USER) {
      rangerPolicyItem = new RangerPolicy.RangerPolicyItem(getAccesses(permissions), prin, null, null, null);
    } else if (principal.getType() == Principal.PrincipalType.GROUP) {
      rangerPolicyItem = new RangerPolicy.RangerPolicyItem(getAccesses(permissions), null, prin, null, null);
    } else {
      throw new RuntimeException("Invalid principal type");
    }
    return ImmutableList.of(rangerPolicyItem);
  }

  List<RangerPolicy.RangerPolicyItemAccess> getAccesses(Set<? extends Permission> permissions) {
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new LinkedList<>();
    for (Permission permission : permissions) {
      accesses.add(new RangerPolicy.RangerPolicyItemAccess(permission.toString().toLowerCase(), true));
    }
    return accesses;
  }

  private void setRangerResource(Authorizable authorizable, Map<String, RangerPolicy.RangerPolicyResource> resource) {
    EntityType entityType = authorizable.getEntityType();
    switch (entityType) {
      case INSTANCE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.INSTANCE)));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        resource.put(RangerCommon.KEY_ARTIFACT,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        break;
      case NAMESPACE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        break;
      case ARTIFACT:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_ARTIFACT,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.ARTIFACT),
                                                           false, false));
        break;
      case APPLICATION:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_APPLICATION,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.APPLICATION)));
        resource.put(RangerCommon.KEY_PROGRAM,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        break;
      case DATASET:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_DATASET,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.DATASET)));
        break;
      case DATASET_MODULE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_DATASET_MODULE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts()
                                                             .get(EntityType.DATASET_MODULE)));
        break;
      case DATASET_TYPE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_DATASET_TYPE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.DATASET_TYPE)));
        break;
      case PROGRAM:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_APPLICATION,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.APPLICATION)));
        resource.put(RangerCommon.KEY_PROGRAM,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.PROGRAM)));
        break;
      case SECUREKEY:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        break;
      case KERBEROSPRINCIPAL:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_PRINCIPAL,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts()
                                                             .get(EntityType.KERBEROSPRINCIPAL)));
        break;
      default:
        throw new IllegalArgumentException(String.format("The authorizable %s is of unknown type %s",
                                                         authorizable, entityType));
    }
  }
}
