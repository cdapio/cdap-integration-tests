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
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.ranger.commons.RangerCommon;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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
 * A Privilege Manager for Ranger which uses Rangers REST V2 API to manage privileges.
 */
public class RangerPrivilegeManager extends AudiTestBase {
  protected static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

  private enum UpdateType {
    GRANT,
    REVOKE
  }

  protected void userGrant(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    List<RangerPolicy> existingPolicies = searchPolicy(entityId);
    if (existingPolicies != null && !existingPolicies.isEmpty()) {
      updatePolicyItems(existingPolicies, entityId, principal, actions, UpdateType.GRANT);
      return;
    }
    // create new policy
    String json = GSON.toJson(createRangerPolicy(entityId, principal, actions));
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
      wr.flush();
      wr.close();
    }
    Assert.assertEquals(conn.getResponseMessage(), HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  protected void userRevoke(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    List<RangerPolicy> existingPolicies = searchPolicy(entityId);
    if (existingPolicies != null && !existingPolicies.isEmpty()) {
      updatePolicyItems(existingPolicies, entityId, principal, actions, UpdateType.REVOKE);
      return;
    }
  }

  protected List<RangerPolicy> searchPolicy(EntityId entityId) throws Exception {
    HttpURLConnection conn = getRangerConnection("service/cdapdev/policy?policyName=" + entityId.toString());
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

  private void updatePolicyItems(List<RangerPolicy> policies, EntityId entityId, Principal principal,
                                 Set<Action> actions, UpdateType updateType) throws IOException {
    if (policies.size() != 1) {
      throw new RuntimeException("No or multiple existing policies found.");
    }
    Long policyId = policies.get(0).getId();
    List<RangerPolicy.RangerPolicyItem> updatedPolicyItems = updatePolicyItems(policies.get(0).getPolicyItems(),
                                                                               principal, actions, updateType);
    String updatedPolicy = GSON.toJson(new RangerPolicy("cdapdev", entityId.toString(), null, "", policies.get(0).getResources(),
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
      wr.flush();
      wr.close();
    }
    Assert.assertEquals(conn.getResponseMessage(), HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  private List<RangerPolicy.RangerPolicyItem> updatePolicyItems(List<RangerPolicy.RangerPolicyItem> existing, Principal
    principal, Set<Action> actions, UpdateType updateType) {
    List<RangerPolicy.RangerPolicyItem> result = new LinkedList<>();
    boolean updatedExistingPrivilege = false;
    for (RangerPolicy.RangerPolicyItem rangerPolicyItem : existing) {
      RangerPolicy.RangerPolicyItem updatedPolicy = rangerPolicyItem;
      HashSet<RangerPolicy.RangerPolicyItemAccess> existingAccesses = Sets.newHashSet(rangerPolicyItem.getAccesses());
      if (principal.getType() == Principal.PrincipalType.GROUP || principal.getType() == Principal.PrincipalType.USER) {
        if (rangerPolicyItem.getGroups().contains(principal.getName().toLowerCase()) ||
          rangerPolicyItem.getUsers().contains(principal.getName().toLowerCase())) {
          updatedExistingPrivilege = true;
          HashSet<RangerPolicy.RangerPolicyItemAccess> updatedPolices = Sets.newHashSet(getAccesses(actions));
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
      result.addAll(getPolicyItems(principal, actions));
    }
    return result;
  }

  private HttpURLConnection getRangerConnection(String urlPart) throws IOException {
    URL url = new URL(getInstanceURI().replace("11015", "6080") + "/service/public/v2/api/" + urlPart);
    return (HttpURLConnection) url.openConnection();
  }

  public RangerPolicy createRangerPolicy(EntityId entityId, Principal principal, Set<Action> actions) {
    Map<String, RangerPolicy.RangerPolicyResource> resource = new LinkedHashMap<>();
    setRangerResource(entityId, resource);
    return new RangerPolicy("cdapdev", entityId.toString(), 0, "", resource, getPolicyItems(principal, actions), null);
  }

  private List<RangerPolicy.RangerPolicyItem> getPolicyItems(Principal principal, Set<Action> actions) {
    List<String> prin = ImmutableList.of(principal.getName());
    RangerPolicy.RangerPolicyItem rangerPolicyItem;
    if (principal.getType() == Principal.PrincipalType.USER) {
      rangerPolicyItem = new RangerPolicy.RangerPolicyItem(getAccesses(actions), prin, null, null, null);
    } else if (principal.getType() == Principal.PrincipalType.GROUP) {
      rangerPolicyItem = new RangerPolicy.RangerPolicyItem(getAccesses(actions), null, prin, null, null);
    } else {
      throw new RuntimeException("Invalid principal type");
    }
    return ImmutableList.of(rangerPolicyItem);
  }

  List<RangerPolicy.RangerPolicyItemAccess> getAccesses(Set<Action> actions) {
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new LinkedList<>();
    for (Action action : actions) {
      accesses.add(new RangerPolicy.RangerPolicyItemAccess(action.toString().toLowerCase(), true));
    }
    return accesses;
  }

  public void setRangerResource(EntityId entityId, Map<String, RangerPolicy.RangerPolicyResource> resource) {
    EntityType entityType = entityId.getEntityType();
    switch (entityType) {
      case INSTANCE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource(((InstanceId) entityId).getInstance()));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        resource.put(RangerCommon.KEY_ARTIFACT,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        setRangerResource(new InstanceId("cdap"), resource);
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(namespaceId.getNamespace(), false, false));
        break;
      case ARTIFACT:
        ArtifactId artifactId = (ArtifactId) entityId;
        setRangerResource(artifactId.getNamespaceId(), resource);
        resource.put(RangerCommon.KEY_ARTIFACT,
                     new RangerPolicy.RangerPolicyResource(artifactId.getArtifact(), false, false));
        break;
      case APPLICATION:
        ApplicationId applicationId = (ApplicationId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(applicationId.getNamespace()));
        resource.put(RangerCommon.KEY_APPLICATION,
                     new RangerPolicy.RangerPolicyResource(applicationId.getApplication()));
        resource.put(RangerCommon.KEY_PROGRAM,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
        break;
      case DATASET:
        DatasetId datasetId = (DatasetId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(datasetId.getNamespace()));
        resource.put(RangerCommon.KEY_DATASET,
                     new RangerPolicy.RangerPolicyResource(datasetId.getDataset()));
        break;
      case DATASET_MODULE:
        DatasetModuleId datasetModuleId = (DatasetModuleId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(datasetModuleId.getNamespace()));
        resource.put(RangerCommon.KEY_DATASET_MODULE,
                     new RangerPolicy.RangerPolicyResource(datasetModuleId.getModule()));
        break;
      case DATASET_TYPE:
        DatasetTypeId datasetTypeId = (DatasetTypeId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(datasetTypeId.getNamespace()));
        resource.put(RangerCommon.KEY_DATASET_TYPE,
                     new RangerPolicy.RangerPolicyResource(datasetTypeId.getType()));
        break;
      case STREAM:
        StreamId streamId = (StreamId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(streamId.getNamespace()));
        resource.put(RangerCommon.KEY_STREAM,
                     new RangerPolicy.RangerPolicyResource(streamId.getStream()));
        break;
      case PROGRAM:

        ProgramId programId = (ProgramId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(programId.getNamespace()));
        resource.put(RangerCommon.KEY_APPLICATION,
                     new RangerPolicy.RangerPolicyResource(programId.getApplication()));
        resource.put(RangerCommon.KEY_PROGRAM,
                     new RangerPolicy.RangerPolicyResource(programId.getType() +
                                                             RangerCommon.RESOURCE_SEPARATOR + programId.getProgram()));
        break;
      case SECUREKEY:
        SecureKeyId secureKeyId = (SecureKeyId) entityId;
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(secureKeyId.getNamespace()));
        resource.put(RangerCommon.KEY_STREAM,
                     new RangerPolicy.RangerPolicyResource(secureKeyId.getName()));
        break;
      case KERBEROSPRINCIPAL:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_PRINCIPAL,
                     new RangerPolicy.RangerPolicyResource(((KerberosPrincipalId) entityId).getPrincipal()));
        setRangerResource(new InstanceId("cdap"), resource);
        break;
      default:
        throw new IllegalArgumentException(String.format("The entity %s is of unknown type %s", entityId, entityType));
    }
  }
}
