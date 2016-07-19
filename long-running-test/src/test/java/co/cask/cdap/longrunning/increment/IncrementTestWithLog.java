///*
// * Copyright © 2015-2016 Cask Data, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//
//package co.cask.cdap.longrunning.increment;
//
//import co.cask.cdap.api.common.Bytes;
//import co.cask.cdap.api.customaction.CustomActionSpecification;
//import co.cask.cdap.api.dataset.lib.KeyValueTable;
//import co.cask.cdap.api.dataset.table.Table;
//import co.cask.cdap.api.workflow.WorkflowActionSpecification;
//import co.cask.cdap.client.ApplicationClient;
//import co.cask.cdap.client.StreamClient;
//import co.cask.cdap.client.config.ClientConfig;
//import co.cask.cdap.client.util.RESTClient;
//import co.cask.cdap.common.NotFoundException;
//import co.cask.cdap.common.ProgramNotFoundException;
//import co.cask.cdap.common.UnauthenticatedException;
//import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
//import co.cask.cdap.common.utils.Tasks;
//import co.cask.cdap.proto.Id;
//import co.cask.cdap.proto.ProgramType;
//import co.cask.cdap.proto.RunRecord;
//import co.cask.cdap.proto.codec.CustomActionSpecificationCodec;
//import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
//import co.cask.cdap.proto.id.DatasetId;
//import co.cask.cdap.test.ApplicationManager;
//import co.cask.cdap.test.FlowManager;
//import co.cask.cdap.test.LongRunningTestBase;
//import co.cask.common.http.HttpMethod;
//import co.cask.common.http.HttpResponse;
//import com.google.common.base.Charsets;
//import com.google.common.collect.Iterables;
//import com.google.common.io.ByteStreams;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import org.junit.Assert;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.StringWriter;
//import java.net.URL;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.Callable;
//import java.util.concurrent.TimeUnit;
//import javax.inject.Inject;
//
///**
// * Tests readless increment functionality of {@link Table}.
// */
//public class IncrementTestWithLog extends LongRunningTestBase<IncrementTestState> {
//  private static final Logger LOG = LoggerFactory.getLogger(IncrementTestWithLog.class);
//
//  private static final int BATCH_SIZE = 100;
//  public static final int SUM_BATCH = (BATCH_SIZE * (BATCH_SIZE - 1)) / 2;
////  private ProgramClient programClient;
//
//  @Override
//  public void deploy() throws Exception {
//    deployApplication(getLongRunningNamespace(), IncrementApp.class);
//  }
//
//  @Override
//  public void start() throws Exception {
//    getApplicationManager().getFlowManager(IncrementApp.IncrementFlow.NAME).start();
//  }
//
//  @Override
//  public void stop() throws Exception {
//    FlowManager flowManager = getApplicationManager().getFlowManager(IncrementApp.IncrementFlow.NAME);
//    flowManager.stop();
//    flowManager.waitForStatus(false);
//  }
//
//  private ApplicationManager getApplicationManager() throws Exception {
//    return getApplicationManager(getLongRunningNamespace().toEntityId().app(IncrementApp.NAME));
//  }
//
//  public String getLastRunLogs() throws Exception {
//    List<RunRecord> runRecords = getApplicationManager().getFlowManager(IncrementApp.IncrementFlow.NAME).getHistory();
//    LOG.info("RUN RECORDS {}", Arrays.toString(runRecords.toArray()));
//    if (runRecords != null) {
//      RunRecord runRecord = Iterables.getLast(runRecords);
//      LOG.info("RUN RECORDS NOT NULL {}", runRecord);
//
//      return new LogClient(getClientConfig(), getRestClient()).getProgramRunLogs
//        (new Id.Program(Id.Application.from(getLongRunningNamespace(), IncrementApp.NAME),
//                        ProgramType.FLOW, IncrementApp.IncrementFlow.NAME),
//         runRecord.getPid());
//    }
//    return null;
//  }
//
//  @Override
//  public IncrementTestState getInitialState() {
//    return new IncrementTestState(0, 0);
//  }
//
//  @Override
//  public void awaitOperations(IncrementTestState state) throws Exception {
//    // just wait until a particular number of events are processed
//    Tasks.waitFor(state.getNumEvents(), new Callable<Long>() {
//      @Override
//      public Long call() throws Exception {
//        DatasetId regularTableId = new DatasetId(getLongRunningNamespace().getId(), IncrementApp.REGULAR_TABLE);
//        KeyValueTable regularTable = getKVTableDataset(regularTableId).get();
//        return readLong(regularTable.read(IncrementApp.NUM_KEY));
//      }
//    }, 5, TimeUnit.MINUTES, 10, TimeUnit.SECONDS);
//  }
//
//  @Override
//  public void verifyRuns(IncrementTestState state) throws Exception {
//    DatasetId readlessTableId = new DatasetId(getLongRunningNamespace().getId(), IncrementApp.READLESS_TABLE);
//    KeyValueTable readlessTable = getKVTableDataset(readlessTableId).get();
//    long readlessSum = readLong(readlessTable.read(IncrementApp.SUM_KEY));
//    long readlessNum = readLong(readlessTable.read(IncrementApp.NUM_KEY));
//    Assert.assertEquals(state.getSumEvents(), readlessSum);
//    Assert.assertEquals(state.getNumEvents(), readlessNum);
//
//    DatasetId regularTableId = new DatasetId(getLongRunningNamespace().getId(), IncrementApp.REGULAR_TABLE);
//    KeyValueTable regularTable = getKVTableDataset(regularTableId).get();
//    long regularSum = readLong(regularTable.read(IncrementApp.SUM_KEY));
//    long regularNum = readLong(regularTable.read(IncrementApp.NUM_KEY));
//    Assert.assertEquals(state.getSumEvents(), regularSum);
//    Assert.assertEquals(state.getNumEvents(), regularNum);
//    LOG.info("HERE are RUN LOGS  {}", getLastRunLogs());
//  }
//
//  @Override
//  public IncrementTestState runOperations(IncrementTestState state) throws Exception {
//    StreamClient streamClient = getStreamClient();
//    LOG.info("Writing {} events in one batch", BATCH_SIZE);
//    StringWriter writer = new StringWriter();
//    for (int i = 0; i < BATCH_SIZE; i++) {
//      writer.write(String.format("%010d", i));
//      writer.write("\n");
//    }
//    streamClient.sendBatch(Id.Stream.from(getLongRunningNamespace(), IncrementApp.INT_STREAM), "text/plain",
//                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));
//    long newSum = state.getSumEvents() + SUM_BATCH;
//    return new IncrementTestState(newSum, state.getNumEvents() + BATCH_SIZE);
//  }
//
//  private long readLong(byte[] bytes) {
//    return bytes == null ? 0 : Bytes.toLong(bytes);
//  }
//
//  /**
//   * Provides ways to interact with CDAP Metadata.
//   */
//  public static class LogClient {
//
//    private static final Gson GSON =
//      (new GsonBuilder()).registerTypeAdapter(WorkflowActionSpecification.class,
//                                              new WorkflowActionSpecificationCodec())
//        .registerTypeAdapter(CustomActionSpecification.class, new CustomActionSpecificationCodec())
//        .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();
//
//    private final RESTClient restClient;
//    private final ClientConfig config;
//    private final ApplicationClient applicationClient;
//
//    @Inject
//    public LogClient(ClientConfig config, RESTClient restClient, ApplicationClient applicationClient) {
//      this.config = config;
//      this.restClient = restClient;
//      this.applicationClient = applicationClient;
//    }
//
//    public LogClient(ClientConfig config) {
//      this(config, new RESTClient(config));
//    }
//
//    public LogClient(ClientConfig config, RESTClient restClient) {
//      this(config, restClient, new ApplicationClient(config, restClient));
//    }
//
//    public String getProgramRunLogs(Id.Program program, String runId) throws IOException,
//      NotFoundException, UnauthenticatedException {
//      String path = String.format("apps/%s/%s/%s/runs/%s/logs",
//                                  program.getApplicationId(), program.getType()
//                                    .getCategoryName(), program.getId(), runId);
//      URL url = this.config.resolveNamespacedURLV3(program.getNamespace(), path);
//      HttpResponse response = this.restClient.execute(HttpMethod.GET, url, this.config.getAccessToken(), new int[0]);
//      LOG.info("RESPONCE {}", response);
//      if (response.getResponseCode() == 404) {
//        throw new ProgramNotFoundException(program);
//      } else {
//        return new String(response.getResponseBody(), Charsets.UTF_8);
//      }
//    }
//  }
//}
