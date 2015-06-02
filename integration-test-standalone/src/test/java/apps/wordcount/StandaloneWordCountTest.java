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
package apps.wordcount;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.apps.wordcount.WordCountTest;
import co.cask.cdap.examples.wordcount.WordCount;
import org.junit.ClassRule;

/**
 * Test for {@link WordCount}.
 */
public class StandaloneWordCountTest extends WordCountTest {
  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @Override
  protected String getInstanceURI() {
    return STANDALONE.getBaseURI().toString();
  }
}
