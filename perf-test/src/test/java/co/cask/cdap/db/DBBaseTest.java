/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.db;

import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.hydrator.common.Constants;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

/**
 * Performs read from source to sink for generic database plugin.
 */
public class DBBaseTest extends DatabaseTestBase {

  @Test
  public void databaseGenericPerfTest() throws Exception {
    testRun();
  }

  protected ETLStage createSource() {
    return new ETLStage("source",
                        new ETLPlugin("Database",
                                      BatchSource.PLUGIN_TYPE,
                                      ImmutableMap.<String, String>builder()
                                        .put("connectionString", System.getProperty("database.connectionString"))
                                        .put("importQuery", System.getProperty("database.importQuery"))
                                        .put("jdbcPluginName", System.getProperty("database.driverName"))
                                        .put("numSplits", "1")
                                        .put("user", System.getProperty("database.user"))
                                        .put("password", System.getProperty("database.password"))
                                        .put(Constants.Reference.REFERENCE_NAME, "DBSource")
                                        .build(),
                                      null));
  }

  protected ETLStage createSink() {
    return new ETLStage("sink",
                        new ETLPlugin("Database",
                                      BatchSink.PLUGIN_TYPE,
                                      ImmutableMap.<String, String>builder()
                                        .put("connectionString", System.getProperty("database.connectionString"))
                                        .put("tableName", System.getProperty("database.sinkTable"))
                                        .put("jdbcPluginName", System.getProperty("database.driverName"))
                                        .put("user", System.getProperty("database.user"))
                                        .put("password", System.getProperty("database.password"))
                                        .put(Constants.Reference.REFERENCE_NAME, "DBSink")
                                        .build(),
                                      null));
  }
}
