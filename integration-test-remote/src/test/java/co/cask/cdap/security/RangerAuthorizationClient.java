package co.cask.cdap.security;

import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.AbstractAuthorizer;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by rsinha on 9/14/17.
 */
public class RangerAuthorizationClient extends AbstractAuthorizer {
  private final AuthorizationClient authorizationClient;
  // this is the cache time out for the authorizer
  private int authorizerCacheTimeout;

  public RangerAuthorizationClient(AuthorizationClient authorizationClient, int authorizerCacheTimeout) {
    this.authorizationClient = authorizationClient;
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
    // the underlying sentry extension which will be called by the authorization client will throw exception
    // if the grant is being done on any other type of principal except Role. We don't need to handle it here
    authorizationClient.grant(authorizable, new Role(principal.getName()), set);
    // Note: this assumes that every user has a group whose name is same as user name and only that particular user
    // belong to it
    authorizationClient.addRoleToPrincipal(new Role(principal.getName()),
                                           new Principal(principal.getName(), Principal.PrincipalType.GROUP));

  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<Action> set) throws Exception {
    authorizationClient.revoke(authorizable, principal.getType() == Principal.PrincipalType.ROLE ? principal : new
      Role(principal.getName()), set);
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

  protected void invalidateCache() throws Exception {
    // this is to make sure the cache times out in both master and remote side
    TimeUnit.SECONDS.sleep(2 * authorizerCacheTimeout + 5);
  }
}
