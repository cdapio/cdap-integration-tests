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

package co.cask.cdap.longrunning.txprune;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.RemoteDatasetAdmin;
import co.cask.chaosmonkey.proto.ClusterDisruptor;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InvalidListPruneTest extends LongRunningTestBase<InvalidListPruneTestState> {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidListPruneTest.class);
  private static final long MAX_EVENTS = 10000000;
  private static final int BATCH_SIZE = 5000;
  private static final int MAX_EMPTY_TABLES = 10; // If this changes, old empty tables may need manual cleanup
  private static final int COMPACT_ITERATION = 5;
  private static final int PRUNE_CHECK_ITERATION = 10;

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), InvalidTxGeneratorApp.class);
  }

  @Override
  public void start() throws Exception {
    // Nothing to start
  }

  @Override
  public void stop() throws Exception {
    // Nothing to stop
  }

  @Override
  public InvalidListPruneTestState getInitialState() {
    return new InvalidListPruneTestState(0, new HashMap<Integer, List<Long>>());
  }

  @Override
  public void awaitOperations(InvalidListPruneTestState state) throws Exception {
    int iteration = state.getIteration();

    // Wait for the previous mapreduce job to finish running
    ApplicationManager applicationManager = getApplicationManager();
    MapReduceManager mrManager = applicationManager.getMapReduceManager(InvalidTxGeneratorApp.InvalidMapReduce.MR_NAME);
    // TODO: Would be good to get only required number of history entries
    mrManager.waitForRuns(ProgramRunStatus.FAILED, iteration, 5, TimeUnit.MINUTES);
  }

  @Override
  public InvalidListPruneTestState verifyRuns(InvalidListPruneTestState state) throws Exception {
    // Verify that the invalid ids from the 5th iteration earlier have been pruned
    // TODO: This check has to be enhanced to take into account test iteration interval, prune interval, tx max lifetime
    int checkIteration = state.getIteration() - PRUNE_CHECK_ITERATION;
    Set<Long> removeIds = new TreeSet<>();
    Map<Integer, List<Long>> newIterationState = new HashMap<>();
    for (Map.Entry<Integer, List<Long>> entry : state.getInvalidTxIds().entrySet()) {
      int iteration = entry.getKey();
      if (iteration < checkIteration) {
        removeIds.addAll(entry.getValue());
      } else {
        newIterationState.put(entry.getKey(), entry.getValue());
      }
    }

    Set<Long> currentInvalids = Sets.newHashSet(getInvalidList());
    Sets.SetView<Long> notRemovedIds = Sets.intersection(currentInvalids, removeIds);
    Assert.assertTrue("Expected the following invalid ids to be pruned: " + notRemovedIds, notRemovedIds.isEmpty());
    LOG.info("Pruned invalid ids: {}", removeIds);

//    Does not work in secure clusters, move it to a service or a worker TODO
//    verifyInvalidDataRemoval(getLongRunningNamespace().dataset(InvalidTxGeneratorApp.DATASET), removeIds);

    return new InvalidListPruneTestState(state.getIteration(), newIterationState);
  }

  @Override
  public InvalidListPruneTestState runOperations(InvalidListPruneTestState state) throws Exception {
    int iteration = state.getIteration() + 1;

    // Flush and compact every COMPACT_ITERATION iterations
    if (iteration % COMPACT_ITERATION == 0) {
      flushAndCompactTables();
    }

    List<Long> invalidList = getInvalidList();

    manageEmptyDatasets(getLongRunningNamespace(), iteration);

    List<String> events = generateStreamEvents(iteration);

    // TODO: Test split and merge table

    truncateAndSendEvents(getLongRunningNamespace().stream(InvalidTxGeneratorApp.STREAM), events);

    // Now run the mapreduce job
    ApplicationManager applicationManager = getApplicationManager();
    applicationManager.getMapReduceManager(InvalidTxGeneratorApp.InvalidMapReduce.MR_NAME).start();

    HashMap<Integer, List<Long>> invalidTxIds = Maps.newHashMap(state.getInvalidTxIds());
    invalidTxIds.put(iteration, invalidList);
    return new InvalidListPruneTestState(iteration, invalidTxIds);
  }

  private List<String> generateStreamEvents(int iteration) {
    // Create unique events for this iteration using the iteration id as part of the event
    List<String> events = new ArrayList<>(BATCH_SIZE + 1);
    for (int i = 0; i < BATCH_SIZE; i++) {
      events.add(String.format("%s", (iteration * MAX_EVENTS) + i));
    }

    // Throw an exception to create an invalid transaction at the end
    events.add(InvalidTxGeneratorApp.EXCEPTION_STRING);
    return Collections.unmodifiableList(events);
  }

  private void truncateAndSendEvents(StreamId stream, List<String> events)
    throws IOException, StreamNotFoundException, UnauthenticatedException, UnauthorizedException {
    StreamClient streamClient = getStreamClient();
    streamClient.truncate(stream);

    StringWriter writer = new StringWriter();
    for (String event : events) {
      writer.write(event);
      writer.write("\n");
    }

    LOG.info("Writing {} events in one batch to stream {}", events.size(), stream);
    streamClient.sendBatch(stream, "text/plain",
                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().app(InvalidTxGeneratorApp.APP_NAME));
  }

  private void flushAndCompactTables() throws Exception {
    ClusterDisruptor clusterDisruptor = getClusterDisruptor();
    clusterDisruptor.disruptAndWait("hbase-master", "major-compact", null, 1000, TimeUnit.SECONDS);
  }

  private List<Long> getInvalidList() throws IOException, UnauthorizedException, UnauthenticatedException {
    RESTClient restClient = getRestClient();
    ClientConfig config = getClientConfig();
    HttpResponse response = restClient.execute(HttpMethod.GET, config.resolveURL("transactions/invalid"),
                                               config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<Long>>() { }).getResponseObject();
  }

  private void manageEmptyDatasets(NamespaceId namespaceId, int iteration) throws Exception {
    // Create an empty table for this iteration
    String baseName = "invalid-tx-empty-table-";
    DatasetId datasetId = namespaceId.dataset(baseName + iteration);
    String typeName = co.cask.cdap.api.dataset.table.Table.class.getName();
    LOG.info("Creating empty dataset {}", datasetId);
    DatasetAdmin datasetAdmin =
      addDatasetInstance(namespaceId, typeName, datasetId.getEntityName(), DatasetProperties.EMPTY);
    Assert.assertTrue("Cannot create empty dataset " + datasetId, datasetAdmin.exists());
    datasetAdmin.close();

    // Now cleanup empty datasets from earlier runs
    int startIteration = iteration - (MAX_EMPTY_TABLES * 5);
    startIteration = startIteration < 0 ? 0 : startIteration;
    for (int i = startIteration; i < iteration - MAX_EMPTY_TABLES; i++) {
      DatasetId instance = namespaceId.dataset(baseName + i);
      DatasetClient datasetClient = getDatasetClient();
      if (datasetClient.exists(instance)) {
        LOG.info("Dropping empty dataset {}", instance);
        datasetClient.delete(instance);
      }
    }
  }
}
