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

package co.cask.cdap.longrunning.logmapreduce;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.LongRunningTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MapReduce LOG testing long running test
 */
public class LogMapReduceTest extends LongRunningTestBase<LogMapReduceTestState> {
  private static final Logger LOG = LoggerFactory.getLogger(LogMapReduceTest.class);

  private static final int BATCH_SIZE = 10;
  private static final String LOG_MAPREDUCE_NAME = "LogMapReducer";
  private List<RunRecord> runRecords = new ArrayList<>();
  private int logFrequency = 10;

  @Override
  public void deploy() throws Exception {
    deployApplication(getLongRunningNamespace(), LogMapReduceApp.class);
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
  }

  @Override
  public LogMapReduceTestState getInitialState() {
    return new LogMapReduceTestState(0);
  }

  @Override
  public void awaitOperations(LogMapReduceTestState state) throws Exception {
    getApplicationManager().getMapReduceManager(LOG_MAPREDUCE_NAME).waitForFinish(5, TimeUnit.MINUTES);
  }

  @Override
  public void verifyRuns(LogMapReduceTestState state) throws Exception {
    String logs = getLastRunLogs();
    if (logs == null) {
      return;
    }
    RunRecord runId = null;
    if (runRecords.size() < logFrequency) {
      runId = runRecords.get(0);
    } else {
      runId = runRecords.get(logFrequency - 1);
    }

    Pattern mapper = Pattern.compile("mapper runid= ".concat(runId.getPid()));
    Pattern reducer = Pattern.compile("reducer ".concat(runId.getPid()));

    Matcher mapperMatcher = mapper.matcher(logs);
    Matcher reducerMatcher = reducer.matcher(logs);

    for (int i = 0; i < logFrequency; i++) {
      boolean mapperMatched = mapperMatcher.find();
      boolean reducerMatched = reducerMatcher.find();
      Assert.assertTrue(mapperMatched && reducerMatched);
    }
  }

  private ApplicationManager getApplicationManager() throws Exception {
    return getApplicationManager(getLongRunningNamespace().toEntityId().app(LogMapReduceApp.NAME));
  }

  @Override
  public LogMapReduceTestState runOperations(LogMapReduceTestState state) throws Exception {
    StreamClient streamClient = getStreamClient();
    long startStream = System.currentTimeMillis();
    StringWriter writer = new StringWriter();
    for (int i = 0; i < BATCH_SIZE; i++) {
      writer.write(String.format("%010d", i));
      writer.write("\n");
    }
    streamClient.sendBatch(Id.Stream.from(getLongRunningNamespace(), LogMapReduceApp.EVENTS_STREAM), "text/plain",
                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));

    // run the mapreduce
    final long startTime = System.currentTimeMillis() + 1;
    Map<String, String> args = Maps.newHashMap();
    args.put("eventReadStartTime", String.valueOf(startStream));
    MapReduceManager mapReduceManager = getApplicationManager().getMapReduceManager("LogMapReducer");
    mapReduceManager.setRuntimeArgs(args);
    mapReduceManager.start(ImmutableMap.of("logical.start.time", Long.toString(startTime)));
    mapReduceManager.waitForFinish(1, TimeUnit.MINUTES);

    return new LogMapReduceTestState(state.getNumRuns() + 1);
  }

  public String getLastRunLogs() throws Exception {

    //each 10th  run//
    runRecords = getApplicationManager().getMapReduceManager(LogMapReducer.NAME).getHistory();
    RunRecord runRecord;
    if (runRecords.size() == 0) {
      return null;
    }
    if (runRecords.size() < logFrequency) {
      runRecord = runRecords.get(0);
    } else {
      runRecord = runRecords.get(logFrequency - 1);
    }
    Id.Program program = new Id.Program(Id.Application.from(getLongRunningNamespace(), LogMapReduceApp.NAME),
                                          ProgramType.MAPREDUCE, LogMapReducer.NAME);

    String path = String.format("apps/%s/%s/%s/runs/%s/logs?format=json", program.getApplicationId(),
                                program.getType().getCategoryName(), program.getId(), runRecord.getPid());
    URL url = getClientConfig().resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = getRestClient()
      .execute(HttpMethod.GET, url, getClientConfig().getAccessToken(), new int[0]);
    if (response.getResponseCode() == 404) {
      throw new ProgramNotFoundException(program);
    } else {
      return new String(response.getResponseBody(), Charsets.UTF_8);
    }
  }
}
