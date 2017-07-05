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

package co.cask.cdap.longrunning.invalidtx.app;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * call service PartitionTableDebugApp.PartitionScannerService GET /pfs/rawRecords
 * start worker PartitionTableDebugApp.PartitionCreateWorker "time=2 concurrency=20 numPartitions=3"
 *
 * flush 'cdap_att:rawRecords.partitions.d'
 * compact 'cdap_att:rawRecords.partitions.d'
 */
public class PartitionCreateWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionCreateWorker.class);

  @Override
  protected void configure() {
    setResources(new Resources(2048));
  }

  @Override
  public void run() {
    String timeString = getContext().getRuntimeArguments().get("time");
    long time = Long.valueOf(timeString);
    String concurrencyString = getContext().getRuntimeArguments().get("concurrency");
    int concurrency = 100;
    if (concurrencyString != null) {
      concurrency = Integer.valueOf(concurrencyString);
    }
    String numPartitionsString = getContext().getRuntimeArguments().get("numPartitions");
    int numPartitions = 10;
    if (numPartitionsString != null) {
      numPartitions = Integer.valueOf(numPartitionsString);
    }
    ListeningExecutorService taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("ingest-many").build()));

    for (int i = 0; i < numPartitions; i++) {
      LOG.info("creating partition " + time + i);
      createPartition(time + i, concurrency, taskExecutorService);
    }

    taskExecutorService.shutdown();
  }

  private void createPartition(final long time, int concurrency,
                               ListeningExecutorService taskExecutorService) {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    for (long i = 0; i < concurrency; i++) {
      futures.add(taskExecutorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext datasetContext) throws Exception {
//              LOG.info("datasetContext: " + datasetContext.getClass()); // multi in Worker, single in service handler
              PFSWrapper pfs = datasetContext.getDataset(PartitionTableDebugApp.RAW_RECORDS);
              addPartitionForTime(pfs, time);
            }
          });
          return null;
        }
      }));
    }

    LOG.info("Submitted {} tasks.", concurrency);
    int numFailures = 0;
    Map<String, Integer> exceptionCounts = new HashMap<>();
    for (ListenableFuture<Void> future : futures) {
      try {
        future.get(20, TimeUnit.SECONDS);
      } catch (Throwable t) {
        if (t instanceof ExecutionException) {
          t = t.getCause();
        }
        numFailures++;
        increment(exceptionCounts, Throwables.getRootCause(t).getClass().getSimpleName());
        LOG.warn("Got exception.", t);
      }
    }
    LOG.info("failures: " + numFailures + ". exceptionMap: " + exceptionCounts);
  }

  private void increment(Map<String, Integer> map, String key) {
    if (map.containsKey(key)) {
      map.put(key, 1 + map.get(key));
    } else {
      map.put(key, 1);
    }
  }

  private void addPartitionForTime(PFSWrapper partitionedFileSet, long time) {
    PartitionKey key = PartitionKey.builder().addLongField("time", time).build();
    if (partitionedFileSet.getPartition(key) == null) {
      PartitionOutput partitionOutput = partitionedFileSet.getPartitionOutput(key);
      partitionOutput.addPartition();
    }
  }
}
