/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;

import javax.annotation.Nullable;

/**
 * App that extends {@link WikipediaPipelineApp} to support Application config to include Programs or Datasets.
 */
public final class WikipediaPipelineTestApp
  extends AbstractApplication<WikipediaPipelineTestApp.WikipediaTestAppConfig> {
  @Override
  public void configure() {
    // At least one program is required for an application to be deployed
    addMapReduce(new StreamToDataset(WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME));
    if (getConfig().createPrograms) {
      addMapReduce(new StreamToDataset(WikipediaPipelineApp.WIKIPEDIA_TO_DATASET_MR_NAME));
      addMapReduce(new WikipediaDataDownloader());
      addMapReduce(new WikiContentValidatorAndNormalizer());
      addMapReduce(new TopNMapReduce());
      addSpark(new SparkWikipediaClustering(getConfig()));
      addWorkflow(new WikipediaPipelineWorkflow(getConfig()));
      addService(new WikipediaService());
    }
    if (getConfig().createDatasets) {
      addStream(new Stream(WikipediaPipelineApp.PAGE_TITLES_STREAM));
      addStream(new Stream(WikipediaPipelineApp.RAW_WIKIPEDIA_STREAM));
      createDataset(WikipediaPipelineApp.PAGE_TITLES_DATASET, KeyValueTable.class,
                    DatasetProperties.builder().setDescription("Page titles dataset").build());
      createDataset(WikipediaPipelineApp.RAW_WIKIPEDIA_DATASET, KeyValueTable.class,
                    DatasetProperties.builder().setDescription("Raw Wikipedia dataset").build());
      createDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET, KeyValueTable.class,
                    DatasetProperties.builder().setDescription("Normalized Wikipedia dataset").build());
      createDataset(WikipediaPipelineApp.SPARK_CLUSTERING_OUTPUT_DATASET, Table.class,
                    DatasetProperties.builder().setDescription("Spark clustering output dataset").build());
      createDataset(WikipediaPipelineApp.MAPREDUCE_TOPN_OUTPUT, KeyValueTable.class,
                    DatasetProperties.builder().setDescription("MapReduce top-'N'-words output dataset").build());
    }
  }

  /**
   * Config for Wikipedia Test App.
   */
  public static final class WikipediaTestAppConfig extends WikipediaPipelineApp.WikipediaAppConfig {
    private final boolean createDatasets;
    private final boolean createPrograms;

    public WikipediaTestAppConfig(@Nullable String clusteringAlgorithm,
                                  boolean createDatasets, boolean createPrograms) {
      super(clusteringAlgorithm);
      this.createDatasets = createDatasets;
      this.createPrograms = createPrograms;
    }
  }
}

