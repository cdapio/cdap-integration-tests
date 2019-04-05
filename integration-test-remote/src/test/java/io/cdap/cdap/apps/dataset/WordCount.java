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

package io.cdap.cdap.apps.dataset;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.dataset.table.TableProperties;

/**
 * Word count sample Application used for integration tests.
 *
 */
public class WordCount extends AbstractApplication<WordCount.WordCountConfig> {

  /**
   * Word Count Application's configuration class.
   */
  public static class WordCountConfig extends Config {
    private String wordStatsTable;

    /**
     * Set default values for the configuration variables.
     */
    public WordCountConfig() {
      this.wordStatsTable = "wordStats";
    }

    /**
     * Used only for unit testing.
     */
    public WordCountConfig(String wordStatsTable) {
      this.wordStatsTable = wordStatsTable;
    }


    public String getWordStatsTable() {
      return wordStatsTable;
    }
  }

  @Override
  public void configure() {
    WordCountConfig config = getConfig();
    setName("WordCount");
    setDescription("Example word count application");

    // Store processed data in Datasets
    createDataset(config.getWordStatsTable(), Table.class,
                  TableProperties.builder()
                    .setReadlessIncrementSupport(true)
                    .setDescription("Stats of total counts and lengths of words")
                    .build());

    // Retrieve the processed data using a Service
    addService(new RetrieveCounts(config));
  }
}
