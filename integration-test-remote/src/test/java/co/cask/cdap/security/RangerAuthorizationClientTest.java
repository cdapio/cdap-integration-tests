package co.cask.cdap.security;

import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Created by rsinha on 9/15/17.
 */
public class RangerAuthorizationClientTest {
  private static RangerAuthorizationClient rangerAuthorizationClient;

  @BeforeClass
  public static void init() {
    rangerAuthorizationClient = new RangerAuthorizationClient(null, System.getProperty("instanceUri"));
  }

  @Test
  public void test() throws Exception {
    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("shankar", Principal.PrincipalType.USER),
                                    ImmutableSet.of
                                      (Action.READ));

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("shankar", Principal.PrincipalType.USER),
                                    ImmutableSet.of
                                      (Action.WRITE));

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("sagar", Principal.PrincipalType.USER),
                                    ImmutableSet.of
                                      (Action.ADMIN));

    List<RangerPolicy> rangerPolicies =
      rangerAuthorizationClient.searchPolicy(Authorizable.fromEntityId(NamespaceId.DEFAULT));

    Assert.assertNotNull(rangerPolicies);
    Assert.assertEquals(1, rangerPolicies.size());
    Assert.assertEquals(NamespaceId.DEFAULT.toString(), rangerPolicies.get(0).getName());
    Assert.assertEquals(2, rangerPolicies.get(0).getPolicyItems().size());
    RangerPolicy.RangerPolicyItem policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(), "shankar");
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.WRITE, Action.READ)),
                        policies.getAccesses());
    policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(), "sagar");
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.ADMIN)), policies.getAccesses());

    rangerAuthorizationClient.revoke(NamespaceId.DEFAULT, new Principal("shankar", Principal.PrincipalType.USER),
                                     ImmutableSet.of(Action.WRITE));

    rangerAuthorizationClient.revoke(NamespaceId.DEFAULT, new Principal("sagar", Principal.PrincipalType.USER),
                                     ImmutableSet.of(Action.ADMIN));

    policies = getPolicyItemsForUser(
      rangerAuthorizationClient.searchPolicy(Authorizable.fromEntityId(NamespaceId.DEFAULT)).get(0).getPolicyItems(),
      "shankar");
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.READ)), policies.getAccesses());


  }

  private RangerPolicy.RangerPolicyItem getPolicyItemsForUser(List<RangerPolicy.RangerPolicyItem> policyItems,
                                                              String username) {
    for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
      if (policyItem.getUsers().contains(username)) {
        return policyItem;
      }
    }
    throw new IllegalArgumentException(String.format("Expected user %s to be present", username));
  }
}
