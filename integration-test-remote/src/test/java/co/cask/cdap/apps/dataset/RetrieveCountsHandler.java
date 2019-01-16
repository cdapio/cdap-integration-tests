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

package co.cask.cdap.apps.dataset;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Retrieve Counts service handler.
 */
public class RetrieveCountsHandler extends AbstractHttpServiceHandler {

  @Property
  private final String wordStatsTableName;


  private Table wordStatsTable;


  public RetrieveCountsHandler(WordCount.WordCountConfig config) {
    this.wordStatsTableName = config.getWordStatsTable();
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    wordStatsTable = context.getDataset(wordStatsTableName);
  }

  /**
   * Returns total number of words, the number of unique words, and the average word length.
   */
  @Path("stats")
  @GET
  public void getStats(HttpServiceRequest request, HttpServiceResponder responder) {
    long totalWords = 0L;
    long uniqueWords = 0L;
    double averageLength = 0.0;

    // Read the total_length and total_words to calculate average length
    Row result = wordStatsTable.get(new Get("totals", "total_length", "total_words"));
    if (!result.isEmpty()) {
      // Extract the total sum of lengths
      long totalLength = result.getLong("total_length", 0);

      // Extract the total count of words
      totalWords = result.getLong("total_words", 0);

      // Compute the average length
      if (totalLength != 0 && totalWords != 0) {
        averageLength = ((double) totalLength) / totalWords;
      }
    }

    // Return a map as JSON
    Map<String, Object> results = new HashMap<>();
    results.put("totalWords", totalWords);
    results.put("uniqueWords", uniqueWords);
    results.put("averageLength", averageLength);

    responder.sendJson(results);
  }
}

