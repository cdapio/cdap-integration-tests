/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.apps.adapters;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.ApplicationTemplateNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import com.google.gson.Gson;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An abstract class for writing Adapters integration tests. Extending classes should provide an implementation to
 * {@link #testAdapters()} method
 */
public abstract class AdaptersTestBase extends AudiTestBase {

  protected static final String DUMMY_STREAM_EVENT = "AAPL|10|500.32";
  protected static final Schema DUMMY_STREAM_EVENT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  protected final AdapterClient adapterClient;
  protected final ETLStageProvider etlStageProvider;
  protected final StreamClient streamClient;
  private static final Gson GSON = new Gson();

  protected AdaptersTestBase() {
    adapterClient = new AdapterClient(getClientConfig(), getRestClient());
    etlStageProvider = getEtlStageProvider();
    streamClient = getStreamClient();
  }

  @Test
  public abstract void testAdapters() throws Exception;

  /**
   * Runs an adapter, wait for its completion and stops it after its completion
   *
   * @param templateId: the {@link Id.ApplicationTemplate} to which this adapter will base adapter off
   * @param adapterId: the {@link Id.Adapter} of the adapter
   * @param etlBatchConfig: the {@link ETLBatchConfig} of the adapter
   * @throws BadRequestException
   * @throws ApplicationTemplateNotFoundException
   * @throws UnauthorizedException
   * @throws IOException
   * @throws AdapterNotFoundException
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  protected void runAndWait(Id.ApplicationTemplate templateId, Id.Adapter adapterId, ETLBatchConfig etlBatchConfig)
    throws BadRequestException, ApplicationTemplateNotFoundException, UnauthorizedException, IOException,
    AdapterNotFoundException, InterruptedException, ExecutionException, TimeoutException {

    AdapterConfig adapterConfig = new AdapterConfig("description", templateId.getId(),
                                                    GSON.toJsonTree(etlBatchConfig));
    adapterClient.create(adapterId, adapterConfig);
    adapterClient.start(adapterId);
    waitForAdapterCompletion(adapterClient, adapterId);
    adapterClient.stop(adapterId);
  }

  /**
   * Creates a {@link Stream} with the given name
   *
   * @param streamName: the name of the stream
   * @return {@link Id.Stream} the id of the created stream
   * @throws UnauthorizedException
   * @throws BadRequestException
   * @throws IOException
   */
  protected Id.Stream createSourceStream(String streamName) throws UnauthorizedException, BadRequestException,
    IOException {
    Id.Stream sourceStreamId = Id.Stream.from(TEST_NAMESPACE, streamName);
    streamClient.create(sourceStreamId);
    return sourceStreamId;
  }

  private void waitForAdapterCompletion(final AdapterClient adapterClient, final Id.Adapter adapterId)
    throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        List<RunRecord> completedRuns =
          adapterClient.getRuns(adapterId, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, null);
        return !completedRuns.isEmpty();
      }
    }, 10, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);
  }
}
