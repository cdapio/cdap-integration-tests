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

package co.cask.cdap.test.suite.category;

import co.cask.cdap.security.AppImpersonationAuthorizationTest;
import co.cask.cdap.security.BasicAppAuthorizationTest;
import co.cask.cdap.security.BasicAuthorizationTest;
import co.cask.cdap.security.CustomMappingAppAuthorizationTest;
import co.cask.cdap.security.CustomMappingBasicAuthorizationTest;
import co.cask.cdap.security.NamespaceImpersonationAppAuthorizationTest;
import co.cask.cdap.security.NamespaceImpersonationBasicAuthorizationTest;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JUnit suite for authorization tests. Runs all of the tests specific to authorization.
 */
@RunWith(Categories.class)
@Suite.SuiteClasses({
  BasicAuthorizationTest.class,
  BasicAppAuthorizationTest.class,
  NamespaceImpersonationBasicAuthorizationTest.class,
  NamespaceImpersonationAppAuthorizationTest.class,
  CustomMappingBasicAuthorizationTest.class,
  CustomMappingAppAuthorizationTest.class,
  AppImpersonationAuthorizationTest.class,
})
public class AuthorizationSuite {
}
