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
package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.utils.Tasks;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An app containing programs which read and emits metadata
 */
public class ProgramMetadataApp extends AbstractApplication {

  static final String APP_NAME = "AppWithMetadataPrograms";
  private static final String INPUT_DATASET = "inputDataset";
  private static final String OUTPUT_DATASET = "outputDataset";


  @Override
  public void configure() {
    setName(APP_NAME);
    addMapReduce(new MetadataMR());
    // TODO: Add more programs here which uses metadata
    // dummy datasets for metadata association
    createDataset(INPUT_DATASET, KeyValueTable.class);
    createDataset(OUTPUT_DATASET, KeyValueTable.class);
  }

  public static class MetadataMR extends AbstractMapReduce {
    public static final String NAME = "MetadataMR";
    private static final int TIMEOUT_IN_SECONDS = 60;
    private static final int SLEEP_DELAY_IN_MILLI_SECONDS = 100;

    @Override
    protected void configure() {
      setName(NAME);
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      context.addInput(Input.ofDataset(INPUT_DATASET));
      context.addOutput(Output.ofDataset(OUTPUT_DATASET));

      // add some tag
      Set<String> tagsToAdd = new HashSet<>(Arrays.asList("tag1", "tag2"));
      addTag(context, INPUT_DATASET, tagsToAdd);
      addTag(context, OUTPUT_DATASET, tagsToAdd);

      // add some properties
      Map<String, String> propertiesToAdd = ImmutableMap.of("k1", "v1", "k2", "v2");
      addProperties(context, INPUT_DATASET, propertiesToAdd);

      Map<MetadataScope, Metadata> inputDsMetadata =
        context.getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), INPUT_DATASET));
      // verify system metadata
      Assert.assertTrue(inputDsMetadata.containsKey(MetadataScope.SYSTEM));
      Assert.assertTrue(inputDsMetadata.containsKey(MetadataScope.USER));
      Assert.assertFalse(inputDsMetadata.get(MetadataScope.SYSTEM).getProperties().isEmpty());
      Assert.assertFalse(inputDsMetadata.get(MetadataScope.SYSTEM).getTags().isEmpty());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.SYSTEM).getProperties().containsKey("entity-name"));
      Assert.assertEquals(INPUT_DATASET,
                          inputDsMetadata.get(MetadataScope.SYSTEM).getProperties().get("entity-name"));
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.SYSTEM).getTags()
                          .containsAll(Arrays.asList("explore", "batch")));
      // verify user metadata
      Assert.assertFalse(inputDsMetadata.get(MetadataScope.USER).getProperties().isEmpty());
      Assert.assertFalse(inputDsMetadata.get(MetadataScope.USER).getTags().isEmpty());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getTags().containsAll(Arrays.asList("tag1", "tag2")));
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getProperties().containsKey("k1"));
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getProperties().containsKey("k2"));
      Assert.assertEquals("v1", inputDsMetadata.get(MetadataScope.USER).getProperties().get("k1"));
      Assert.assertEquals("v2", inputDsMetadata.get(MetadataScope.USER).getProperties().get("k2"));

      // verify output dataset only have user tag and not properties
      Map<MetadataScope, Metadata> outputDsMetadata =
        context.getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), OUTPUT_DATASET));
      Assert.assertTrue(outputDsMetadata.get(MetadataScope.USER).getProperties().isEmpty());
      Assert.assertFalse(outputDsMetadata.get(MetadataScope.USER).getTags().isEmpty());
      Assert.assertTrue(outputDsMetadata.get(MetadataScope.USER).getTags().containsAll(Arrays.asList("tag1", "tag2")));

      // delete a tag
      removeTag(context, INPUT_DATASET, "tag1");

      // delete a property
      removeProperty(context, INPUT_DATASET, "k1");

      // get metadata and verify
      inputDsMetadata = context.getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), INPUT_DATASET));
      Assert.assertEquals(1, inputDsMetadata.get(MetadataScope.USER).getTags().size());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getTags().contains("tag2"));
      Assert.assertEquals(1, inputDsMetadata.get(MetadataScope.USER).getProperties().size());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getProperties().containsKey("k2"));
      Assert.assertEquals("v2", inputDsMetadata.get(MetadataScope.USER).getProperties().get("k2"));

      // remove all properties and tags
      removeProperties(context, INPUT_DATASET);
      removeTags(context, INPUT_DATASET);

      // get metadata and verify
      inputDsMetadata = context.getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), INPUT_DATASET));
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getTags().isEmpty());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getProperties().isEmpty());

      // remove all metadata from output dataset
      removeAllMetadata(context, OUTPUT_DATASET);
      // get metadata and verify
      inputDsMetadata = context.getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), OUTPUT_DATASET));
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getTags().isEmpty());
      Assert.assertTrue(inputDsMetadata.get(MetadataScope.USER).getProperties().isEmpty());
    }

    private void addTag(MapReduceContext context, String dataset, Set<String> tags) throws InterruptedException,
      ExecutionException, TimeoutException {
      for (String tag : tags) {
        context.addTags(MetadataEntity.ofDataset(context.getNamespace(), dataset), tag);
      }
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata.get(MetadataScope.USER).getTags().containsAll(tags);
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    private void addProperties(MapReduceContext context, String dataset, Map<String, String> properties)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.addProperties(MetadataEntity.ofDataset(context.getNamespace(), dataset), properties);
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return metadata.get(MetadataScope.USER).getProperties().keySet().containsAll(properties.keySet());
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }


    private void removeTag(MapReduceContext context, String dataset, String tag)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.removeTags(MetadataEntity.ofDataset(context.getNamespace(), dataset), tag);

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return !metadata.get(MetadataScope.USER).getTags().contains(tag);
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    private void removeTags(MapReduceContext context, String dataset)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.removeTags(MetadataEntity.ofDataset(context.getNamespace(), dataset));

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return metadata.get(MetadataScope.USER).getTags().isEmpty();
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    private void removeProperty(MapReduceContext context, String dataset, String key)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.removeProperties(MetadataEntity.ofDataset(context.getNamespace(), dataset), key);

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return !metadata.get(MetadataScope.USER).getProperties().keySet().contains(key);
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    private void removeProperties(MapReduceContext context, String dataset)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.removeProperties(MetadataEntity.ofDataset(context.getNamespace(), dataset));

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return metadata.get(MetadataScope.USER).getProperties().isEmpty();
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    private void removeAllMetadata(MapReduceContext context, String dataset)
      throws InterruptedException, ExecutionException, TimeoutException {
      context.removeMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));

      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Map<MetadataScope, Metadata> metadata =
            context.getMetadata(MetadataEntity.ofDataset(context.getNamespace(), dataset));
          return metadata.get(MetadataScope.USER).getProperties().isEmpty() &&
            metadata.get(MetadataScope.USER).getTags().isEmpty();
        }
      }, TIMEOUT_IN_SECONDS, TimeUnit.SECONDS, SLEEP_DELAY_IN_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }
  }
}
