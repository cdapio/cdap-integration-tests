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

package co.cask.cdap.longrunning.datacleansing;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * DataCleansing Application which extends {@link co.cask.cdap.examples.datacleansing.DataCleansing} to assert using
 * readless table instead of metrics.
 */
public class DataCleansingApp extends co.cask.cdap.examples.datacleansing.DataCleansing {
  /**
   Copied from {@link co.cask.cdap.examples.datacleansing.DataCleansing} as values are protected
   */
  public static final String RAW_RECORDS = "rawRecords";
  public static final String CLEAN_RECORDS = "cleanRecords";
  public static final String INVALID_RECORDS = "invalidRecords";
  public static final String CONSUMING_STATE = "consumingState";
  public static final String TOTAL_RECORDS_TABLE = "totalRecords";

  public static final byte[] CLEAN_RECORD_KEY = {'c'};
  public static final byte[] INVALID_RECORD_KEY = {'i'};

  @Override
  public void configure() {
    super.configure();
    this.addMapReduce(new DataCleansingMapReduce());
    createDataset(TOTAL_RECORDS_TABLE, KeyValueTable.class, DatasetProperties.builder()
      .add(Table.PROPERTY_READLESS_INCREMENT, "true").build());
  }
}
