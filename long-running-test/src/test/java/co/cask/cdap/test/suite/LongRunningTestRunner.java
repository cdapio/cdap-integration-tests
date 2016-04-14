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

package co.cask.cdap.test.suite;

import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.runner.AutoSuiteRunner;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Suite to run all long running tests for CDAP
 */
@RunWith(AutoSuiteRunner.class)
@AutoSuiteRunner.Matches(packages = "co.cask.cdap.longrunning")
public class LongRunningTestRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LongRunningTestRunner.class);
  private static final String INPUT_STATE_PROP = "input.state";
  private static final String OUTPUT_STATE_PROP = "output.state";
  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void deserialize() throws Exception {
    String inputStateFile = System.getProperty(INPUT_STATE_PROP);
    Preconditions.checkNotNull(inputStateFile, "Input state file name cannot be null.");
    LOG.info("Input state file = {}", inputStateFile);

    String outputStateFile = System.getProperty(OUTPUT_STATE_PROP);
    Preconditions.checkNotNull(outputStateFile, "Output state file name cannot be null.");
    LOG.info("Output state file = {}", outputStateFile);

    Type type = new TypeToken<Map<String, String>>() { }.getType();
    Map<String, String> inMemoryMap;
    try (FileReader fileReader = new FileReader(inputStateFile)) {
      LOG.info("Deserializing test state from input file = {}", inputStateFile);
      inMemoryMap = GSON.fromJson(fileReader, type);
    } catch (FileNotFoundException e) {
      LOG.warn("Input state file not found, creating new instance of in memory map");
      inMemoryMap = new HashMap<>();
    }
    LongRunningTestBase.initializeInMemoryMap(inMemoryMap);
  }

  @AfterClass
  public static void serialize() throws Exception {
    String outputStateFile = System.getProperty(OUTPUT_STATE_PROP);
    try (FileWriter writer = new FileWriter(outputStateFile)) {
      LOG.info("Serializing test state to output file = {}", outputStateFile);
      GSON.toJson(LongRunningTestBase.getInMemoryMap(), writer);
    }
  }
}
