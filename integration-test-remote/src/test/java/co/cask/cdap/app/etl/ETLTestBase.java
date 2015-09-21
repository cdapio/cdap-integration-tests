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

package co.cask.cdap.app.etl;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.realtime.ETLRealtimeApplication;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.common.base.Throwables;
import org.junit.Before;

import java.io.IOException;

/**
 * An abstract class for writing etl integration tests. Tests for etl should extend this class.
 */
public abstract class ETLTestBase extends AudiTestBase {

  protected static final String DUMMY_STREAM_EVENT = "AAPL|10|500.32";
  protected static final Schema DUMMY_STREAM_EVENT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  protected ETLStageProvider etlStageProvider;
  protected StreamClient streamClient;
  protected ApplicationClient appClient;
  protected DatasetClient datasetClient;
  protected String version;

  @Before
  public void setup() {
    appClient = getApplicationClient();
    datasetClient = getDatasetClient();
    etlStageProvider = new ETLStageProvider();
    streamClient = getStreamClient();
  }

  protected AppRequest<ETLBatchConfig> getBatchAppRequest(ETLBatchConfig config)
    throws IOException, UnauthorizedException {
    return new AppRequest<>(new ArtifactSummary("cdap-etl-batch", getVersion(), ArtifactScope.SYSTEM), config);
  }

  protected AppRequest<ETLRealtimeConfig> getRealtimeAppRequest(ETLRealtimeConfig config)
    throws IOException, UnauthorizedException {
    return new AppRequest<>(new ArtifactSummary("cdap-etl-realtime", getVersion(), ArtifactScope.SYSTEM), config);
  }

  private String getVersion() {
    if (version == null) {
      try {
        version = getMetaClient().getVersion().getVersion();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
    return version;
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
}
