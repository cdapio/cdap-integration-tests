/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.app.etl.realtime;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.realtime.ETLWorker;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.plugin.common.Properties;
import co.cask.hydrator.plugin.realtime.source.DataGeneratorSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link co.cask.cdap.etl.realtime.ETLRealtimeApplication}.
 */
public class ETLWorkerTest extends ETLTestBase {

  @Test
  public void testEmptyProperties() throws Exception {
    ETLStage source = new ETLStage("DataGeneratorSource",
                                   new ETLPlugin("DataGenerator",
                                                 RealtimeSource.PLUGIN_TYPE,
                                                 ImmutableMap.of(Constants.Reference.REFERENCE_NAME, "DataGenerator"),
                                                 null));
    // Set properties to null to test if ETLTemplate can handle it.
    ETLStage transform = new ETLStage("Projection",
                                      new ETLPlugin("Projection",
                                                    Transform.PLUGIN_TYPE,
                                                    ImmutableMap.<String, String>of(),
                                                    null));
    ETLStage sink = new ETLStage("StreamSink",
                                 new ETLPlugin("Stream",
                                               RealtimeSink.PLUGIN_TYPE,
                                               ImmutableMap.of(Properties.Stream.NAME, "testS"),
                                               null));

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("testAdap");
    ApplicationManager appManager = deployApplication(appId, getRealtimeAppRequest(etlConfig));
    Assert.assertNotNull(appManager);
  }

  @Test
  @Category(SlowTests.class)
  public void testStreamSinks() throws Exception {
    ETLPlugin sourceConfig =
      new ETLPlugin("DataGenerator",
                    RealtimeSource.PLUGIN_TYPE,
                    ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.STREAM_TYPE,
                                    Constants.Reference.REFERENCE_NAME, "DataGenerator"),
                    null);
    ETLStage source = new ETLStage("DataGeneratorSource", sourceConfig);

    ETLStage sink1 = new ETLStage("StreamA", new ETLPlugin("Stream",
                                                           RealtimeSink.PLUGIN_TYPE,
                                                           ImmutableMap.of(Properties.Stream.NAME, "streamA"),
                                                           null));
    ETLStage sink2 = new ETLStage("StreamB", new ETLPlugin("Stream",
                                                           RealtimeSink.PLUGIN_TYPE,
                                                           ImmutableMap.of(Properties.Stream.NAME, "streamA"),
                                                           null));
    ETLStage sink3 = new ETLStage("StreamC", new ETLPlugin("Stream",
                                                           RealtimeSink.PLUGIN_TYPE,
                                                           ImmutableMap.of(Properties.Stream.NAME, "streamA"),
                                                           null));

    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .setInstances(1)
      .addStage(source)
      .addStage(sink1)
      .addStage(sink2)
      .addStage(sink3)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .addConnection(source.getName(), sink3.getName())
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = getRealtimeAppRequest(etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    long startTime = System.currentTimeMillis();
    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);
    workerManager.start();
    workerManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    List<StreamManager> streamManagers = Lists.newArrayList(
      getTestManager().getStreamManager(TEST_NAMESPACE.stream("streamA")),
      getTestManager().getStreamManager(TEST_NAMESPACE.stream("streamB")),
      getTestManager().getStreamManager(TEST_NAMESPACE.stream("streamC"))
    );

    int retries = 0;
    boolean succeeded = false;
    while (retries < PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS) {
      succeeded = checkStreams(streamManagers, startTime);
      if (succeeded) {
        break;
      }
      retries++;
      TimeUnit.SECONDS.sleep(1);
    }

    workerManager.stop();
    Assert.assertTrue(succeeded);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTableSink() throws Exception {
    ETLPlugin sourceConfig =
      new ETLPlugin("DataGenerator",
                    RealtimeSource.PLUGIN_TYPE,
                    ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE, DataGeneratorSource.TABLE_TYPE,
                                    Constants.Reference.REFERENCE_NAME, "DataGenerator"),
                    null);
    ETLPlugin sinkConfig =
      new ETLPlugin("Table",
                    RealtimeSink.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.Table.NAME, "table1",
                                    Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary"),
                    null);
    ETLStage source = new ETLStage("DataGenSource", sourceConfig);
    ETLStage sink = new ETLStage("TableSink", sinkConfig);
    ETLRealtimeConfig etlConfig = ETLRealtimeConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("testToStream");
    AppRequest<ETLRealtimeConfig> appRequest = getRealtimeAppRequest(etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    workerManager.start();
    DataSetManager<Table> tableManager = getTableDataset("table1");
    workerManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    waitForTableToBePopulated(tableManager);
    workerManager.stop();

    // verify
    Table table = tableManager.get();
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertEquals(1, (int) row.getInt("id"));
    Assert.assertEquals("Bob", row.getString("name"));
    Assert.assertEquals(3.4, row.getDouble("score"), 0.000001);
    Assert.assertEquals(false, row.getBoolean("graduated"));
    Assert.assertNotNull(row.getLong("time"));
  }

//  TODO: testKafkaSource

  private void waitForTableToBePopulated(final DataSetManager<Table> tableManager) throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Table table = tableManager.get();
        Row row = table.get("Bob".getBytes(Charsets.UTF_8));
        // need to wait for information to get to the table, not just for the row to be created
        return row.getColumns().size() != 0;
      }
    }, 10, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }

  private boolean checkStreams(Collection<StreamManager> streamManagers, long startTime) throws IOException {
    try {
      long currentDiff = System.currentTimeMillis() - startTime;
      for (StreamManager streamManager : streamManagers) {
        List<StreamEvent> streamEvents = streamManager.getEvents("now-" + Long.toString(currentDiff) + "ms", "now",
                                                                 Integer.MAX_VALUE);
        // verify that some events were sent to the stream
        if (streamEvents.size() == 0) {
          return false;
        }
        // since we sent all identical events, verify the contents of just one of them
        Random random = new Random();
        StreamEvent event = streamEvents.get(random.nextInt(streamEvents.size()));
        ByteBuffer body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        if (headers != null && !headers.isEmpty()) {
          // check h1 header has value v1
          if (!"v1".equals(headers.get("h1"))) {
            return false;
          }
        }
        // check body has content "Hello"
        if (!"Hello".equals(Bytes.toString(body, Charsets.UTF_8))) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      // streamManager.getEvents() can throw an exception if there is nothing in the stream
      return false;
    }
  }
}
