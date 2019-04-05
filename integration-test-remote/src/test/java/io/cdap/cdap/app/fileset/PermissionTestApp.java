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

package io.cdap.cdap.app.fileset;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.lib.DynamicPartitioner;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * An application used to test file system permissions set by datasets.
 * <ul>
 * <li>Uses a Service that uploads files, and allows to list the contents of a location and their permissions.
 * <li>Includes a MapReduce that implements the classic word count example. It can write to a file set,
 *     a partitioned file set, and a time-partitioned file set, with dynamic partitioning and multiple outputs.
 * </li>
 * </ul>
 */
public class PermissionTestApp extends AbstractApplication {

  static final String TEMP = "tempdataset";
  static final String INPUT = "input";
  static final String FS = "fileset";
  static final String PFS = "pfs";
  static final String TPFS = "tpfs";
  static final String INPUT_PATH = "some/input.txt";
  static final String SERVICE = "FileService";
  static final String MAPREDUCE = "Tokenizer";

  @Override
  public void configure() {
    setName("PermissionApp");
    setDescription("Application with a MapReduce that uses a FileSet dataset");
    addMapReduce(new Tokenizer());
    addService(new AbstractService() {
      @Override
      protected void configure() {
          setName(SERVICE);
          addHandler(new FileHandler());
      }
    });
  }

  @SuppressWarnings("WeakerAccess")
  public static class Listing {
    private final String path;
    private final String group;
    private final String permission;
    private final List<Listing> children;

    public Listing(String path, String group, String permission, List<Listing> children) {
      this.path = path;
      this.group = group;
      this.permission = permission;
      this.children = children;
    }

    public String getPath() {
      return path;
    }

    public String getGroup() {
      return group;
    }

    public String getPermission() {
      return permission;
    }

    public List<Listing> getChildren() {
      return children;
    }
  }

  /**
   * A handler to create datasets, upload files and list locations with permissions and groups.
   */
  public static class FileHandler extends AbstractHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FileHandler.class);

    private static Listing list(Location location) throws IOException {
      List<Listing> children = null;
      if (location.isDirectory()) {
        children = new ArrayList<>();
        for (Location child : location.list()) {
          children.add(list(child));
        }
      }
      return new Listing(location.toURI().getPath(), location.getGroup(), location.getPermissions(), children);
    }

    /**
     * Responds with the list of groups of the user that runs the service.
     */
    @GET
    @Path("groups")
    public void list(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      String[] groups = UserGroupInformation.getCurrentUser().getGroupNames();
      responder.sendJson(200, groups);
    }

    /**
     * Responds with the recursive listing of the file set named in the path.
     */
    @GET
    @Path("list/{name}")
    public void list(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("name") String name,
                     @QueryParam("path") String path,
                     @QueryParam("time") Long time,
                     @QueryParam("key") String key) {

      FileSet fileset;
      Dataset dataset;
      try {
        dataset = getContext().getDataset(name);
        if (dataset instanceof FileSet) {
          fileset = (FileSet) dataset;
        } else if (dataset instanceof PartitionedFileSet) {
          fileset = ((PartitionedFileSet) dataset).getEmbeddedFileSet();
        } else {
          responder.sendError(400, String.format("Dataset '%s' has wrong type %s", name, dataset.getClass().getName()));
          return;
        }
      } catch (DatasetInstantiationException e) {
        LOG.error("Error instantiating dataset {}", name, e);
        responder.sendError(400, String.format("Cannot instantiate dataset '%s'", name));
        return;
      }
      Location location;
      if (path != null) {
        location = fileset.getLocation(path);
      } else if (time != null) {
        //noinspection ConstantConditions
        location = ((TimePartitionedFileSet) dataset).getPartitionByTime(time).getLocation();
      } else if (key != null) {
        location = fileset.getLocation(key);
      } else {
        location = fileset.getBaseLocation();
      }
      try {
        responder.sendJson(200, list(location));
      } catch (IOException e) {
        LOG.error("Error listing dataset '{}'", name, e);
        responder.sendError(500, String.format("Error listing dataset '%s': %s", name, e.getMessage()));
      }
    }

    /**
     * Upload the content for a new file at the location specified by the request.
     */
    @PUT
    @Path("input")
    public void write(HttpServiceRequest request, HttpServiceResponder responder,
                      @QueryParam("path") String path) {
      try {
        FileSet fileSet = getContext().getDataset(INPUT);
        path = path != null ? path : INPUT_PATH;
        try (WritableByteChannel out = Channels.newChannel(fileSet.getLocation(path).getOutputStream())) {
          out.write(request.getContent());
        }
        responder.sendStatus(200);
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'. Reason: '%s'",
                                               path, INPUT, e.getMessage()));
      }
    }
  }

  public static class Tokenizer extends AbstractMapReduce {

    private static final Logger LOG = LoggerFactory.getLogger(Tokenizer.class);

    @Override
    public void configure() {
      setName(MAPREDUCE);
      setMapperResources(new Resources(1024));
    }

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(TokenMapper.class);
      job.setNumReduceTasks(0);

      Map<String, String> inputArguments = new HashMap<>();
      FileSetArguments.addInputPath(inputArguments, INPUT_PATH);
      context.addInput(Input.ofDataset(INPUT, inputArguments));
      String umask = context.getRuntimeArguments().get("umask");

      String path = context.getRuntimeArguments().get("path");
      String time = context.getRuntimeArguments().get("time");
      String key = context.getRuntimeArguments().get("key");

      if (path != null) {
        Map<String, String> outputArguments = new HashMap<>();
        FileSetArguments.setOutputPath(outputArguments, path);
        LOG.info("Adding output dataset '{}' with arguments  {}", FS, outputArguments);
        context.addOutput(Output.ofDataset(FS, outputArguments));
      }
      if (time != null) {
        Map<String, String> outputArguments = new HashMap<>();
        TimePartitionedFileSetArguments.setOutputPartitionTime(outputArguments, Long.parseLong(time));
        LOG.info("Adding output dataset '{}' with arguments  {}", TPFS, outputArguments);
        context.addOutput(Output.ofDataset(TPFS, outputArguments));
      }
      if (key != null) {
        Map<String, String> outputArguments = new HashMap<>();
        PartitionedFileSetArguments.setDynamicPartitioner(outputArguments, KeyPartitioner.class);
        LOG.info("Adding output dataset '{}' with arguments  {}", PFS, outputArguments);
        context.addOutput(Output.ofDataset(PFS, outputArguments));
      }
      if (umask != null) {
        LOG.info("Setting umask to {}", umask);
        job.getConfiguration().set(FsPermission.UMASK_LABEL, umask); // "fs.permissions.umask-mode"
      }
    }

    public static final class KeyPartitioner extends DynamicPartitioner<String, Long> {

      private String x;

      @Override
      public void initialize(MapReduceTaskContext<String, Long> mapReduceTaskContext) {
        super.initialize(mapReduceTaskContext);
        x = mapReduceTaskContext.getRuntimeArguments().get("key");
      }

      @Override
      public PartitionKey getPartitionKey(String key, Long value) {
        return PartitionKey.builder()
          .addStringField("x", x)
          .addStringField("y", key.substring(0, 1))
          .build();
      }
    }

    /**
     * A mapper that splits each input line and emits each token with a value of 1.
     */
    public static class TokenMapper extends Mapper<LongWritable, Text, String, Long>
      implements ProgramLifecycle<MapReduceTaskContext<String, Long>> {

      private MapReduceTaskContext<String, Long> taskContext;
      private boolean doPfs, doTpfs, doFs, namedOutput;

      @Override
      public void initialize(MapReduceTaskContext<String, Long> context) throws Exception {
        taskContext = context;
        // use named outputs if there are at least two outputs
        doPfs = context.getRuntimeArguments().containsKey("key");
        doTpfs = context.getRuntimeArguments().containsKey("time");
        doFs = context.getRuntimeArguments().containsKey("path");
        namedOutput = (doFs && (doPfs || doTpfs)) || (doPfs && doTpfs);
      }

      @Override
      public void map(LongWritable key, Text data, Context context)
        throws IOException, InterruptedException {
        for (String word : data.toString().split(" ")) {
          write (doFs, FS, word, context);
          write (doPfs, PFS, word, context);
          write (doTpfs, TPFS, word, context);
        }
      }

      private void write(boolean doWrite, String name, String word, Context context)
        throws IOException, InterruptedException {
        if (doWrite) {
          if (namedOutput) {
            taskContext.write(name, word, 1L);
          } else {
            context.write(word, 1L);
          }
        }
      }

      @Override
      public void destroy() { }
    }
  }
}
