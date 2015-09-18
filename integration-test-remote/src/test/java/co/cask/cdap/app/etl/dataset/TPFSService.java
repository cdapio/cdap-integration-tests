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

package co.cask.cdap.app.etl.dataset;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

public class TPFSService extends AbstractService {

  public static final String TPFS_PATH = "tpfs";
  public static final String TPFS_1 = TPFS_PATH + "1";
  public static final String TPFS_2 = TPFS_PATH + "2";
  public static final Schema EVENT_SCHEMA = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @Override
  protected void configure() {
    setDescription("A Service to read the TPFS files");
    addHandler(new TPFSHandler());
  }

  public static class TPFSHandler extends AbstractHttpServiceHandler {

    @UseDataSet(TPFS_1)
    @SuppressWarnings("unused")
    private TimePartitionedFileSet tpfs1;

    @UseDataSet(TPFS_2)
    @SuppressWarnings("unused")
    private TimePartitionedFileSet tpfs2;

    @GET
    @Path(TPFS_PATH + "/{tpfsName}")
    public void readTPFS(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("tpfsName") String tpfsName, @QueryParam("time") long time) throws IOException {

      Set<TimePartitionDetail> timePartitionDetailSet;
      TimePartitionedFileSet tpfs;
      try {
        tpfs = getContext().getDataset(tpfsName);
      } catch (DatasetInstantiationException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, String.format("Invalid file set name '%s'", tpfsName));
        return;
      }
      timePartitionDetailSet = tpfs.getPartitionsByTime(time, System.currentTimeMillis()
                                                                                        + TimeUnit.MINUTES.toMillis(1));

      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
        .parse(TPFSService.EVENT_SCHEMA.toString());

      DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(avroSchema);
      List<GenericData.Record> records = Lists.newArrayList();
      if (!timePartitionDetailSet.isEmpty()) {
        TimePartitionDetail tpd = (TimePartitionDetail) (timePartitionDetailSet.toArray())[0];
        for (Location dayLoc : tpd.getLocation().list()) {
          String locName = dayLoc.getName();

          if (locName.endsWith(".avro")) {
            DataFileStream<GenericData.Record> fileStream =
              new DataFileStream<>(dayLoc.getInputStream(), datumReader);
            Iterables.addAll(records, fileStream);
            fileStream.close();
          }
        }
      }
      responder.send(HttpURLConnection.HTTP_OK, ByteBuffer.wrap(Bytes.toBytes(records.toString())),
                     "application/octet-stream", ImmutableMap.<String, String>of());
    }
  }
}
