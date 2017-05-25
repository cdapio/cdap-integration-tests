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
package co.cask.cdap.test;

import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Preconditions;
import org.junit.Before;

import java.io.IOException;

/**
 * Common names, methods and setups for all Auth tests
 */
public class AuthTestBase extends AudiTestBase {
  protected static final String ADMIN_USER = "cdapitn";
  protected static final String ALICE = "alice";
  protected static final String BOB = "bob";
  protected static final String CAROL = "carol";
  protected static final String EVE = "eve";
  protected static final String PASSWORD_SUFFIX = "password";
  protected static final String NO_PRIVILEGE_MSG = "unable to obtain groups for carol";
  protected static final String NO_PRIVILEGE_MSG_ACCESS = "does not have privileges to access";

  @Before
  public void setup() throws UnauthorizedException, IOException, UnauthenticatedException {
    ConfigEntry configEntry = this.getMetaClient().getCDAPConfig().get("security.authorization.enabled");
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s",
                               "security.authorization.enabled");
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "Authorization not enabled.");
  }

}

