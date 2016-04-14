/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Abstract class for writing long running tests for CDAP.
 *
 * @param <T> Type of test state
 */
public abstract class LongRunningTestBase<T extends TestState> extends AudiTestBase implements LongRunningTest<T> {
  public static final Logger LOG = LoggerFactory.getLogger(LongRunningTestBase.class);
  // key is Test class name and value is test state in json format
  private static Map<String, String> inMemoryStatePerTest;
  private static final Gson GSON = new Gson();

  private static final String STAGE = System.getProperty("stage");
  private static final String PRE = "PRE";
  private static final String POST = "POST";

  private Id.Namespace longRunningNamespace;
  private T state;

  public static void initializeInMemoryMap(Map<String, String> inMemoryMap) {
    inMemoryStatePerTest = inMemoryMap;
  }

  private Id.Namespace configureLongRunningNamespace(String namespace) throws Exception {
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    if (!getNamespaceClient().exists(namespaceId)) {
      createNamespace(namespace);
    }
    return namespaceId;
  }

  public static Map<String, String> getInMemoryMap() {
    return inMemoryStatePerTest;
  }

  public Id.Namespace getLongRunningNamespace() {
    return longRunningNamespace;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    checkSystemServices();
    longRunningNamespace =
      configureLongRunningNamespace(System.getProperty("long.running.namespace", NamespaceId.DEFAULT.getNamespace()));

    boolean firstRun = false;
    Type stateType = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    String key = getTestName();
    if (inMemoryStatePerTest.containsKey(key)) {
      state = GSON.fromJson(inMemoryStatePerTest.get(key), stateType);
    } else {
      LOG.warn("Input state not found, treating this as the first run");
      firstRun = true;
      state = getInitialState();
      inMemoryStatePerTest.put(key, GSON.toJson(state));
    }

    LOG.info("Got input state = {}", state);

    if (firstRun) {
      LOG.info("Executing first run of long running test {}...", getTestName());
      deploy();
      start();
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    inMemoryStatePerTest.put(getTestName(), GSON.toJson(state));
  }

  private String getTestName() {
    return getClass().getCanonicalName();
  }

  @Test
  public void test() throws Exception {
    if (Boolean.getBoolean("longrunning.as.upgrade")) {
      testUpgrade();
    } else {
      testLongRunning();
    }
  }

  private void testLongRunning() throws Exception {
    LOG.info("Running one iteration of long running test {}", getTestName());
    runOneIteration();
  }

  private void testUpgrade() throws Exception {
    LOG.info("Testing stage {} of Upgrade: {}.", STAGE, getTestName());
    if (POST.equalsIgnoreCase(STAGE)) {
      // in POST stage, we need to start up the test application, even though there exists a state file
      // because the PRE stage stopped everything at the end (in order for CDAP to be stopped and upgraded).
      // we can remove the call to deploy, if we want to test whether users don't have to redeploy apps after an
      // upgrade.
      deploy();
      start();
    } else if (!PRE.equalsIgnoreCase(STAGE)) {
      throw new IllegalArgumentException(String.format("Unknown stage: %s. Allowed stages: %s, %s", STAGE, PRE, POST));
    }
    try {
      // run two iterations and then verify runs at the end (don't need to runOperations at the end)
      runOneIteration();
      runOneIteration();

      LOG.info("Calling awaitOperations...");
      awaitOperations(state);
      verifyRuns(state);
    } finally {
      try {
        stop();
      } catch (Exception e) {
        // simply log exceptions from the stop. Otherwise, it'll mask any actual exceptions from the try block
        LOG.error("Got exception while stopping.", e);
      }
    }
    LOG.info("Testing stage {} of Upgrade: {} - SUCCESSFUL!", STAGE, getTestName());
  }

  private void runOneIteration() throws Exception {
    LOG.info("Running one iteration of test run {}", getTestName());

    LOG.info("Calling awaitOperations...");
    awaitOperations(state);
    LOG.info("Calling verifyRuns...");
    verifyRuns(state);
    LOG.info("Calling runOperations...");
    state = runOperations(state);
    LOG.info("Got output state = {}", state);

    LOG.info("One iteration of test run {} completed", getTestName());
  }
}
