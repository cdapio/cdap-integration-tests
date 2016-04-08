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

import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.proto.Id;
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
  private Id.Namespace longRunningNamespace;
  private static final Gson GSON = new Gson();

  public static void initializeInMemoryMap(Map<String, String> inMemoryMap) {
    inMemoryStatePerTest = inMemoryMap;
  }

  private Id.Namespace configureLongRunningNamespace(String namespaceId) throws Exception {
    Id.Namespace testNamespace = Id.Namespace.DEFAULT;
    if (namespaceId != null) {
      NamespaceClient namespaceClient = getNamespaceClient();
      try {
        testNamespace = Id.Namespace.from(namespaceClient.get(Id.Namespace.from(namespaceId)).getName());
      } catch (NamespaceNotFoundException e) {
        testNamespace = createNamespace(namespaceId);
      }
    }
    return testNamespace;
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
    longRunningNamespace = configureLongRunningNamespace(System.getProperty("long.running.namespace"));
    checkSystemServices();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    checkSystemServices();
  }

  @Test
  public void test() throws Exception {
    LOG.info("Starting test run {}", getClass().getCanonicalName());

    boolean firstRun = false;
    T inputState;
    Type stateType = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    String key = getClass().getCanonicalName();
    if (inMemoryStatePerTest.containsKey(key)) {
      inputState = GSON.fromJson(inMemoryStatePerTest.get(key), stateType);
    } else {
      LOG.warn("Input state not found, treating this as the first run");
      firstRun = true;
      inputState = getInitialState();
      inMemoryStatePerTest.put(key, GSON.toJson(inputState));
    }

    LOG.info("Got input state = {}", inputState);

    if (firstRun) {
      LOG.info("Calling setup...");
      setup();
    }

    LOG.info("Calling verifyRuns...");
    verifyRuns(inputState);
    LOG.info("Calling runOperations...");
    T outputState = runOperations(inputState);

    LOG.info("Got output state = {}", outputState);
    inMemoryStatePerTest.put(key, GSON.toJson(outputState));

    LOG.info("Test run {} completed", getClass().getCanonicalName());
  }
}
