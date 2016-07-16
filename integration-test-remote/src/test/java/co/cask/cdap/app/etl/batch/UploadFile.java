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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Application to upload file and return it's location.
 */
public class UploadFile extends AbstractApplication {

  @Override
  public void configure() {
    setName("UploadFile");
    setDescription("Application to upload file on HDFS using FileSet dataset");
    addService(new FileSetService());

  }

  public static class FileSetService extends AbstractService {

    @Override
    protected void configure() {
      setName("FileSetService");
      setDescription("A service to upload files to file sets, and return it's location");
      setInstances(1);
      addHandler(new FileSetService.FileSetHandler());
    }

    /**
     * Handler to upload file and returns it's location.
     */
    public static class FileSetHandler extends AbstractHttpServiceHandler {
      private static final Gson GSON = new Gson();
      private static final Logger LOG = LoggerFactory.getLogger(FileSetService.FileSetHandler.class);

      /**
       * Responds with location of the file specified by the request.
       *
       * @param set      the name of the file set
       * @param filePath the relative path within the file set
       */
      @GET
      @Path("{fileset}")
      public void read(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("fileset") String set, @QueryParam("path") String filePath) {

        FileSet fileSet;
        try {
          fileSet = getContext().getDataset(set);
        } catch (DatasetInstantiationException e) {
          LOG.warn("Error instantiating file set {}", set, e);
          responder.sendError(400, String.format("Invalid file set name '%s'", set));
          return;
        }

        Location location = fileSet.getLocation(filePath);
        getContext().discardDataset(fileSet);

        try {
          responder.sendString(location.toURI().toString());
        } catch (Exception e) {
          responder.sendError(400, String.format("Unable to read path '%s' in file set '%s'", filePath, set));
        }
      }

      /**
       * Check if file exist or not.
       * Respond with true for exist otherwise false.
       *
       * @param set      the name of the file set
       * @param filePath the relative path within the file set
       */
      @GET
      @Path("fileExist/{fileset}")
      public void fileExist(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("fileset") String set, @QueryParam("path") String filePath) {
        FileSet fileSet;
        try {
          fileSet = getContext().getDataset(set);
        } catch (DatasetInstantiationException e) {
          LOG.warn("Error instantiating file set {}", set, e);
          responder.sendError(400, String.format("Invalid file set name '%s'", set));
          return;
        }

        Location location = fileSet.getLocation(filePath);
        getContext().discardDataset(fileSet);

        try {
          responder.sendString(String.valueOf(location.exists()));
        } catch (Exception e) {
          responder.sendError(400, String.format("Unable to read path '%s' in file set '%s'", filePath, set));
        }
      }

      /**
       * Upload the content for a new file at the location specified by the request.
       *
       * @param set      the name of the file set
       * @param filePath the relative path within the file set
       */
      @PUT
      @Path("{fileset}")
      public HttpContentConsumer write(HttpServiceRequest request, HttpServiceResponder responder,
                                       @PathParam("fileset") final String set,
                                       @QueryParam("path") final String filePath) {
        FileSet fileSet;
        try {
          fileSet = getContext().getDataset(set);
        } catch (DatasetInstantiationException e) {
          LOG.warn("Error instantiating file set {}", set, e);
          responder.sendError(400, String.format("Invalid file set name '%s'", set));
          return null;
        }

        final Location location = fileSet.getLocation(filePath);
        getContext().discardDataset(fileSet);
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
              responder.sendStatus(200);
            }

            @Override
            public void onError(HttpServiceResponder responder, Throwable failureCause) {
              Closeables.closeQuietly(channel);
              try {
                location.delete();
              } catch (IOException e) {
                LOG.warn("Failed to delete {}", location, e);
              }
              LOG.debug("Unable to write path '{}' in file set '{}'", filePath, set, failureCause);
              responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'. Reason: '%s'",
                                                     filePath, set, failureCause.getMessage()));
            }
          };
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'. Reason: '%s'",
                                                 filePath, set, e.getMessage()));
          return null;
        }
      }

      /**
       * Create a new file set. The properties for the new dataset can be given as JSON in the body
       * of the request. Alternatively the request can specify the name of an existing dataset as a query
       * parameter; in that case, a copy of the properties of that dataset is used to create the new file set.
       * If neither a body nor a clone parameter is present, the dataset is created with empty (that is, default)
       * properties.
       *
       * @param set   the name of the file set
       * @param clone the name of an existing dataset. If present, its properties are used for the new dataset.
       */
      @POST
      @Path("{fileset}/create")
      public void create(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("fileset") final String set,
                         @Nullable @QueryParam("clone") final String clone) throws DatasetManagementException {
        DatasetProperties properties = DatasetProperties.EMPTY;
        ByteBuffer content = request.getContent();
        if (clone != null) {
          try {
            properties = getContext().getAdmin().getDatasetProperties(clone);
          } catch (InstanceNotFoundException e) {
            responder.sendError(404, "Dataset '" + clone + "' does not exist");
            return;
          }
        } else if (content != null && content.hasRemaining()) {
          try {
            properties = GSON.fromJson(Bytes.toString(content), DatasetProperties.class);
          } catch (Exception e) {
            responder.sendError(400, "Invalid properties: " + e.getMessage());
            return;
          }
        }
        try {
          getContext().getAdmin().createDataset(set, "fileSet", properties);
        } catch (InstanceConflictException e) {
          responder.sendError(409, "Dataset '" + set + "' already exists");
          return;
        }
        responder.sendStatus(200);
      }
    }
  }
}
