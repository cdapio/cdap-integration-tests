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

package co.cask.cdap.test.suite;

import co.cask.cdap.security.AppImpersonationTest;
import co.cask.cdap.security.CrossNSAppImpersonationTest;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JUnit suite for impersonation tests. Runs all of the tests in {@link AllTests} and impersonation-specific tests.
 */
@RunWith(Categories.class)
@Suite.SuiteClasses({
  // TODO: Currently impersonation integration test runs on CDH5.5 clusters only. Once we support running on different
  // distros, this should be AllTests.class
  CDH55Suite.class,
  AppImpersonationTest.class,
  CrossNSAppImpersonationTest.class
})
public class ImpersonationSuite {
}
