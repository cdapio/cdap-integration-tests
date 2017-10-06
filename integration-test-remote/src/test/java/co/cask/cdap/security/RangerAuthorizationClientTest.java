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
 * Test for {@link RangerAuthorizationClient}.
 * This tests tests that we can successfully use the client to grant/revoke privileges to and from user.
 * Note: This test does not run in any ITN. It is just a unit test written for {@link RangerAuthorizationClient}
 * testing. To run it you should set -DinstanceUri to the instance URI of the cluster. It also assumes that cdap
 * master and
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

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("sysadmin", Principal.PrincipalType.GROUP),
                                    ImmutableSet.of(Action.ADMIN));

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("sysadmin", Principal.PrincipalType.GROUP),
                                    ImmutableSet.of(Action.WRITE));

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("ali", Principal.PrincipalType.USER),
                                    ImmutableSet.of(Action.ADMIN));

    rangerAuthorizationClient.grant(NamespaceId.DEFAULT, new Principal("ali", Principal.PrincipalType.USER),
                                    ImmutableSet.of(Action.WRITE));


    List<RangerPolicy> rangerPolicies =
      rangerAuthorizationClient.searchPolicy("policyName=" + Authorizable.fromEntityId(NamespaceId.DEFAULT));

    Assert.assertNotNull(rangerPolicies);
    Assert.assertEquals(1, rangerPolicies.size());
    Assert.assertEquals(NamespaceId.DEFAULT.toString(), rangerPolicies.get(0).getName());
    Assert.assertEquals(4, rangerPolicies.get(0).getPolicyItems().size());
    RangerPolicy.RangerPolicyItem policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(), new
      Principal("shankar", Principal.PrincipalType.USER));
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.WRITE, Action.READ)),
                        policies.getAccesses());
    policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(),
                                     new Principal("sagar", Principal.PrincipalType.USER));
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.ADMIN)), policies.getAccesses());

    policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(), new
      Principal("sysadmin", Principal.PrincipalType.GROUP));
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.WRITE, Action.ADMIN)),
                        policies.getAccesses());

    policies = getPolicyItemsForUser(rangerPolicies.get(0).getPolicyItems(), new
      Principal("ali", Principal.PrincipalType.USER));
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.WRITE, Action.ADMIN)),
                        policies.getAccesses());

    rangerAuthorizationClient.revoke(NamespaceId.DEFAULT, new Principal("shankar", Principal.PrincipalType.USER),
                                     ImmutableSet.of(Action.WRITE));

    rangerAuthorizationClient.revoke(NamespaceId.DEFAULT, new Principal("sagar", Principal.PrincipalType.USER),
                                     ImmutableSet.of(Action.ADMIN));

    policies = getPolicyItemsForUser(
      rangerAuthorizationClient.searchPolicy("policyName=" +
                                               Authorizable.fromEntityId(NamespaceId.DEFAULT)).get(0).getPolicyItems(),
      new Principal("shankar", Principal.PrincipalType.USER));
    Assert.assertEquals(rangerAuthorizationClient.getAccesses(ImmutableSet.of(Action.READ)), policies.getAccesses());

    rangerAuthorizationClient.revokeAll("shankar");

    List<RangerPolicy> shankarPolcies = rangerAuthorizationClient.searchPolicy("user=shankar");
    Assert.assertEquals(0, shankarPolcies.size());

    // cleanup
    rangerAuthorizationClient.revokeAll("sagar");
    rangerAuthorizationClient.revokeAll("ali");
  }

  @Test
  public void del() throws Exception {
    rangerAuthorizationClient.revokeAll("rsinha");
    rangerAuthorizationClient.revokeAll("cdapitn");
    rangerAuthorizationClient.revokeAll("alice");
    rangerAuthorizationClient.revokeAll("bob");
    rangerAuthorizationClient.revokeAll("carol");
    rangerAuthorizationClient.revokeAll("eve");
    rangerAuthorizationClient.revokeAll("cdap");
    rangerAuthorizationClient.revokeAll("systemadmin");
  }


  private RangerPolicy.RangerPolicyItem getPolicyItemsForUser(List<RangerPolicy.RangerPolicyItem> policyItems,
                                                              Principal principal) {
    for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
      if (principal.getType() == Principal.PrincipalType.USER) {
        if (policyItem.getUsers().contains(principal.getName())) {
          return policyItem;
        }
      } else if (principal.getType() == Principal.PrincipalType.GROUP) {
        if (policyItem.getGroups().contains(principal.getName())) {
          return policyItem;
        }
      } else {
        throw new IllegalArgumentException("Illegal principal type");
      }
    }
    throw new IllegalArgumentException(String.format("Expected user %s to be present", principal.getName()));
  }
}
