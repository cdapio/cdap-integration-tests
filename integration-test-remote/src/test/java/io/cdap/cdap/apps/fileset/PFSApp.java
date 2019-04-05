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

package io.cdap.cdap.apps.fileset;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionOutput;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.api.dataset.lib.app.partitioned.PartitionExploreCorrector;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.http.HttpStatus;
import org.apache.twill.filesystem.Location;

import java.io.OutputStream;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Simple app with partitioned file set.
 */
public class PFSApp extends AbstractApplication {

  @Override
  public void configure() {
    setDescription("Simple app with partitioned file set");
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName("PFSService");
        addHandler(new PFSHandler());
      }
    });
    addWorker(new PartitionExploreCorrector.PartitionWorker());
    createDataset("pfs", PartitionedFileSet.class.getName(),
                  PartitionedFileSetProperties.builder()
                    .setPartitioning(Partitioning.builder().addStringField("key").build())
                    .setInputFormat(TextInputFormat.class)
                    .setOutputFormat(TextOutputFormat.class)
                    .setEnableExploreOnCreate(true)
                    .setExploreFormat("csv")
                    .setExploreSchema("a string, b string")
                    .setDescription("a pfs with text files")
                    .build());
  }

  public static class PFSHandler extends AbstractHttpServiceHandler {

    @Path("{key}")
    @PUT
    public void create(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("key") String key) throws Exception {

      PartitionKey partitionKey = PartitionKey.builder().addField("key", key).build();
      PartitionedFileSet pfs = getContext().getDataset("pfs");
      if (pfs.getPartition(partitionKey) != null) {
        responder.sendStatus(HttpStatus.SC_CONFLICT);
        return;
      }
      PartitionOutput output = pfs.getPartitionOutput(partitionKey);
      Location partLocation = output.getLocation().append("part1");
      try (OutputStream out = partLocation.getOutputStream()) {
        String line = String.format("%s,%s:%s\n", key, key, key);
        out.write(Bytes.toBytes(line));
      }
      output.addPartition();
      responder.sendStatus(HttpStatus.SC_OK);
    }
  }
}
