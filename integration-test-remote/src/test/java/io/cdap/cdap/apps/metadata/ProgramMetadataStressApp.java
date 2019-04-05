/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.cdap.apps.metadata;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.test.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An app containing a MapReduce jobs which emits and delete a lot of metadata to stress the MetadataService running
 * in dataset op executor
 */
public class ProgramMetadataStressApp extends AbstractApplication {

  static final String APP_NAME = "AppWithMetadataProgramsStress";
  private static final Logger LOG = LoggerFactory.getLogger(ProgramMetadataStressApp.class);
  private static final String INPUT_DATASET = "inputDatasetStress";
  private static final String OUTPUT_DATASET = "outputDatasetStress";
  // TODO: This should be bumped up for larger cluster to do stress test. Make this configurable through
  // app config pass by the test from vm arguments.
  private static final Integer LOAD = 10;


  @Override
  public void configure() {
    setName(APP_NAME);
    addMapReduce(new StressMetadataMR());
    // dummy datasets for metadata association
    createDataset(INPUT_DATASET, KeyValueTable.class);
    createDataset(OUTPUT_DATASET, KeyValueTable.class);
  }

  public static class StressMetadataMR extends AbstractMapReduce {
    public static final String NAME = "StressMetadataMR";

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      context.addInput(Input.ofDataset(INPUT_DATASET));
      context.addOutput(Output.ofDataset(OUTPUT_DATASET));

      MetadataEntity inputDS = MetadataEntity.ofDataset(getContext().getNamespace(), INPUT_DATASET);

      LOG.info("Stress testing with adding tags");
      for (int i = 0; i < LOAD; i++) {
        MetadataEntity field = getFieldEntity(inputDS, i);
        context.addTags(field, Integer.toString(i));
      }

      LOG.info("Waiting for all tags to be processed");
      // wait for last field tag to be processed
      waitForProcessing(getFieldEntity(inputDS, LOAD - 1), ImmutableSet.of(Integer.toString(LOAD - 1)));

      LOG.info("Stress testing with remove tags");
      // remove tags
      for (int i = 0; i < LOAD; i++) {
        MetadataEntity field = getFieldEntity(inputDS, i);
        context.removeTags(field);
      }

      LOG.info("Waiting for all tags to be removed");
      waitForProcessing(getFieldEntity(inputDS, LOAD - 1), Collections.emptySet());
    }

    private void waitForProcessing(MetadataEntity metadataEntity, Set<String> expectedTags)
      throws InterruptedException, ExecutionException, TimeoutException {
      Tasks.waitFor(true, () -> {
        Map<MetadataScope, Metadata> metadata =
          getContext().getMetadata(metadataEntity);
        return expectedTags.isEmpty() ? metadata.get(MetadataScope.USER).getTags().isEmpty() :
          metadata.get(MetadataScope.USER).getTags().containsAll(expectedTags);
      }, 5, TimeUnit.MINUTES, 100, TimeUnit.MILLISECONDS);
    }

    private MetadataEntity getFieldEntity(MetadataEntity inputDS, int field) {
      return MetadataEntity.builder(inputDS).appendAsType("field",
                                                          Integer.toString(field)).build();
    }
  }
}
