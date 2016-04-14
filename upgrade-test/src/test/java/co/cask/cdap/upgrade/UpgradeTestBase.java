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

package co.cask.cdap.upgrade;

import co.cask.cdap.test.AudiTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test base to test cdap functionality after an upgrade.
 */
public abstract class UpgradeTestBase extends AudiTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(UpgradeTestBase.class);
  private static final String STAGE = System.getProperty("stage");
  private static final String PRE = "PRE";
  private static final String POST = "POST";

  protected abstract void preStage() throws Exception;
  protected abstract void postStage() throws Exception;

  @Before
  public void setUp() throws Exception {
    // override super to avoid calls to assertUnrecoverableResetEnabled and assertIsClear
    checkSystemServices();
  }

  @After
  public void tearDown() throws Exception {
    // override super to avoid unrecoverable reset at the end
  }

  @Test
  public final void testUpgrade() throws Exception {
    LOG.info("Testing stage {} of Upgrade: {}.", STAGE, getClass().getCanonicalName());
    if (PRE.equalsIgnoreCase(STAGE)) {
      preStage();
    } else if (POST.equalsIgnoreCase(STAGE)) {
      postStage();
    } else {
      throw new IllegalArgumentException(String.format("Unknown stage: %s. Allowed stages: %s, %s", STAGE, PRE, POST));
    }
    LOG.info("Testing stage {} of Upgrade: {} - SUCCESSFUL!", STAGE, getClass().getCanonicalName());
  }
}
