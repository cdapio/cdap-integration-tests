/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.standalone;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.test.runner.AutoSuiteRunner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(AutoSuiteRunner.class)
@AutoSuiteRunner.Matches(packages = "co.cask.cdap.apps.wordcount")
public class StandaloneTestSuite {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @BeforeClass
  public static void init() {
    System.setProperty("instanceUri", STANDALONE.getBaseURI().toString());
  }
}
