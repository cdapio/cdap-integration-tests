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

package co.cask.cdap.longrunning.logmapreduce;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * An application that illustrates
 * periodic stream conversion.
 */
public class LogMapReduceApp extends AbstractApplication {

  public static final String NAME = "LogMapReduceApp";
  public static final String EVENTS_STREAM = "events";

  @Override
  public void configure() {
    addStream(new Stream(EVENTS_STREAM));
    addMapReduce(new LogMapReducer());
    createDataset("converted", KeyValueTable.class);
  }
}
