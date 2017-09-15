package co.cask.cdap.security;

import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.authorization.ranger.commons.RangerCommon;
import co.cask.cdap.security.spi.authorization.AbstractAuthorizer;
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
 * Created by rsinha on 9/14/17.
 */
public class RangerAuthorizationClient extends AbstractAuthorizer {
  protected static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();

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
  public void createRole(Role role) throws Exception {
    authorizationClient.createRole(role);
  }

  @Override
  public void dropRole(Role role) throws Exception {
    authorizationClient.dropRole(role);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws Exception {
    authorizationClient.addRoleToPrincipal(role, principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws Exception {
    authorizationClient.removeRoleFromPrincipal(role, principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws Exception {
    return authorizationClient.listRoles(principal);
  }

  @Override
  public Set<Role> listAllRoles() throws Exception {
    return authorizationClient.listAllRoles();
  }

  @Override
  public void enforce(EntityId entityId, Principal principal, Set<Action> set) throws Exception {
    authorizationClient.enforce(entityId, principal, set);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> set, Principal principal) throws Exception {
    return authorizationClient.isVisible(set, principal);
  }


  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<Action> set) throws Exception {
    List<RangerPolicy> existingPolicies = searchPolicy(authorizable);
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
      wr.flush();
      wr.close();
    }
    Assert.assertEquals(conn.getResponseMessage(), HttpURLConnection.HTTP_OK, conn.getResponseCode());

  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<Action> set) throws Exception {
    List<RangerPolicy> existingPolicies = searchPolicy(authorizable);
    if (existingPolicies != null && !existingPolicies.isEmpty()) {
      updatePolicyItems(existingPolicies, authorizable, principal, set, UpdateType.REVOKE);
      return;
    }
  }

  @Override
  public void revoke(Authorizable authorizable) throws Exception {
    authorizationClient.revoke(authorizable);
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    grant(Authorizable.fromEntityId(entity), principal, actions);
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    revoke(Authorizable.fromEntityId(entity), principal, actions);
  }

  @Override
  public void revoke(EntityId entity) throws Exception {
    revoke(Authorizable.fromEntityId(entity));
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) throws Exception {
    return authorizationClient.listPrivileges(principal);
  }


  protected List<RangerPolicy> searchPolicy(Authorizable authorizable) throws Exception {
    HttpURLConnection conn = getRangerConnection("service/cdapdev/policy?policyName=" + authorizable.toString());
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
                                 Set<Action> actions, UpdateType updateType) throws IOException {
    if (policies.size() != 1) {
      throw new RuntimeException("No or multiple existing policies found.");
    }
    Long policyId = policies.get(0).getId();
    List<RangerPolicy.RangerPolicyItem> updatedPolicyItems = updatePolicyItems(policies.get(0).getPolicyItems(),
                                                                               principal, actions, updateType);
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
    URL url = new URL(instaceURI.replace("11015", "6080") + "/service/public/v2/api/" + urlPart);
    return (HttpURLConnection) url.openConnection();
  }

  public RangerPolicy createRangerPolicy(Authorizable authorizable, Principal principal, Set<Action> actions) {
    Map<String, RangerPolicy.RangerPolicyResource> resource = new LinkedHashMap<>();
    setRangerResource(authorizable, resource);
    return new RangerPolicy("cdapdev", authorizable.toString(), 0, "", resource, getPolicyItems(principal, actions), null);
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

  public void setRangerResource(Authorizable authorizable, Map<String, RangerPolicy.RangerPolicyResource> resource) {
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
        resource.put(RangerCommon.KEY_STREAM,
                     new RangerPolicy.RangerPolicyResource("*", true, false));
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
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.DATASET_MODULE)));
        break;
      case DATASET_TYPE:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_DATASET_TYPE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.DATASET_TYPE)));
        break;
      case STREAM:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_NAMESPACE,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.NAMESPACE)));
        resource.put(RangerCommon.KEY_STREAM,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.STREAM)));
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
        resource.put(RangerCommon.KEY_STREAM,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.SECUREKEY)));
        break;
      case KERBEROSPRINCIPAL:
        resource.put(RangerCommon.KEY_INSTANCE,
                     new RangerPolicy.RangerPolicyResource("cdap"));
        resource.put(RangerCommon.KEY_PRINCIPAL,
                     new RangerPolicy.RangerPolicyResource(authorizable.getEntityParts().get(EntityType.KERBEROSPRINCIPAL)));
        break;
      default:
        throw new IllegalArgumentException(String.format("The authorizable %s is of unknown type %s",
                                                         authorizable, entityType));
    }
  }
}
