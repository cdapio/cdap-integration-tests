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

package co.cask.cdap.test;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Abstract class for writing long running tests for CDAP.
 *
 * @param <T> Type of test state
 */
public abstract class LongRunningTestBase<T extends TestState> extends AudiTestBase implements LongRunningTest<T> {
  public static final Logger LOG = LoggerFactory.getLogger(LongRunningTestBase.class);
  public static final String INPUT_STATE_PROP = "input.state";
  public static final String OUTPUT_STATE_PROP = "output.state";

  private static final Gson GSON = new Gson();

  @Before
  @Override
  public void setUp() throws Exception {
    checkSystemServices();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    checkSystemServices();
  }

  // TODO: call cleanup and reset
  @Test
  public void test() throws Exception {
    LOG.info("Starting test run {}", getClass().getCanonicalName());
    String inputStateFile = System.getProperty(INPUT_STATE_PROP);
    LOG.info("Input state file = {}", inputStateFile);
    Preconditions.checkNotNull(inputStateFile, "Input state file name cannot be null.");

    String outputStateFile = System.getProperty(OUTPUT_STATE_PROP);
    LOG.info("Output state file = {}", outputStateFile);
    Preconditions.checkNotNull(outputStateFile, "Output state file name cannot be null.");

    boolean firstRun = false;
    T inputState;
    Type stateType = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    try (FileReader fileReader = new FileReader(inputStateFile)) {
      inputState = GSON.fromJson(fileReader, stateType);
    } catch (FileNotFoundException e) {
      LOG.warn("Input state file not found, treating this as the first run");
      firstRun = true;
      inputState = getInitialState();
    }

    LOG.info("Got input state = {}", inputState);

    if (firstRun) {
      // TODO: would be a good idea to call operations separately than calling super.setUp()
      super.setUp();
      LOG.info("Calling setup...");
      setup();
    }

    LOG.info("Calling verifyRuns...");
    verifyRuns(inputState);
    LOG.info("Calling runOperations...");
    T outputState = runOperations(inputState);

    LOG.info("Got output state = {}", outputState);
    try (FileWriter writer = new FileWriter(outputStateFile)) {
      GSON.toJson(outputState, writer);
    }
    LOG.info("Test run {} completed", getClass().getCanonicalName());
  }
}
