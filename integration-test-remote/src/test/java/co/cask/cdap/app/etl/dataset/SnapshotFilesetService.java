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

package co.cask.cdap.app.etl.dataset;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.hydrator.plugin.dataset.SnapshotFileSet;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Service to read PartitionedFileset
 */
public class SnapshotFilesetService extends AbstractService {
  public static final String ITEM_DATASET = "itemSink";
  public static final String USER_DATASET = "userSink";

  @Override
  protected void configure() {
    setDescription("A Service to read the PartitionedFileset files");
    addHandler(new SnapshotFilesetHandler());
  }

  public static class SnapshotFilesetHandler extends AbstractHttpServiceHandler {
    
    @GET
    @Path("read/{datasetName}")
    public void readFileset(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("datasetName") String datasetName) throws IOException {
      PartitionedFileSet fileset;

      try {
        fileset = getContext().getDataset(datasetName);
      } catch (DatasetInstantiationException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            String.format("Invalid file set name '%s'", datasetName));
        return;
      }

      try {
        Location partitionLocation = new SnapshotFileSet(fileset).getLocation();
        responder.sendJson(readOutput(partitionLocation));
      } catch (InterruptedException e) {
        responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
                            String.format("Error while reading fileset '%s'", datasetName));
      }
    }

    private Map<String, byte[]> readOutput(Location outputLocation) throws IOException {
      Map<String, byte[]> fileContents = new HashMap<>();
      for (Location file : outputLocation.list()) {
        if (file.getName().endsWith(".avro")) {
          InputStream inputStream = file.getInputStream();
          fileContents.put(file.getName(), ByteStreams.toByteArray(inputStream));
        }
      }
      return fileContents;
    }
  }
}
