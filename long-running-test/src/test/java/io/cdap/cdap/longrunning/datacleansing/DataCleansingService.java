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

package io.cdap.cdap.longrunning.datacleansing;

import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionOutput;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpContentConsumer;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A {@link Service} to write to PartitionedFileSet.
 */
public class DataCleansingService extends AbstractService {

  public static final String NAME = "DataCleansingService";

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A service to ingest data into the rawRecords partitioned file set.");
    addHandler(new RecordsHandler());
  }

  /**
   * A handler that allows writing to the 'rawRecords' PartitionedFileSet.
   */
  @Path("/v1")
  public static class RecordsHandler extends AbstractHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RecordsHandler.class);

    @SuppressWarnings("unused")
    @UseDataSet(DataCleansing.RAW_RECORDS)
    private PartitionedFileSet rawRecords;

    @POST
    @Path("/records/raw")
    public HttpContentConsumer write(HttpServiceRequest request, HttpServiceResponder responder) {
      PartitionKey key = PartitionKey.builder().addLongField("time", System.currentTimeMillis()).build();
      final PartitionOutput partitionOutput = rawRecords.getPartitionOutput(key);
      final Location location = partitionOutput.getLocation();
      try {
        final WritableByteChannel channel = Channels.newChannel(location.getOutputStream());
        return new HttpContentConsumer() {
          @Override
          public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
            channel.write(chunk);
          }

          @Override
          public void onFinish(HttpServiceResponder responder) throws Exception {
            channel.close();
            partitionOutput.addPartition();
            responder.sendStatus(200);
          }

          @Override
          public void onError(HttpServiceResponder responder, Throwable failureCause) {
            try {
              channel.close();
            } catch (IOException e) {
              LOG.warn("Failed to close {}", channel, e);
            }
            try {
              location.delete();
            } catch (IOException e) {
              LOG.warn("Failed to delete {}", location, e);
            }
            LOG.debug("Unable to write path '{}'", location, failureCause);
            responder.sendError(400, String.format("Unable to write path '%s'. Reason: '%s'",
                                                   location, failureCause.getMessage()));
          }
        };
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s'. Reason: '%s'", location, e.getMessage()));
        return null;
      }
    }
  }
}
