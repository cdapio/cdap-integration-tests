/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.dataset;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.TimePartitionDetail;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
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
                         @PathParam("tpfsName") String tpfsName, @QueryParam("startTime") long startTime,
                         @QueryParam("endTime") long endTime) throws IOException {

      Set<TimePartitionDetail> timePartitionDetailSet;
      TimePartitionedFileSet tpfs;
      try {
        tpfs = getContext().getDataset(tpfsName);
      } catch (DatasetInstantiationException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, String.format("Invalid file set name '%s'", tpfsName));
        return;
      }

      timePartitionDetailSet = tpfs.getPartitionsByTime(startTime, endTime);
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser()
        .parse(TPFSService.EVENT_SCHEMA.toString());

      List<GenericRecord> records = Lists.newArrayList();
      if (!timePartitionDetailSet.isEmpty()) {
        TimePartitionDetail tpd = (TimePartitionDetail) (timePartitionDetailSet.toArray())[0];
        for (Location dayLoc : tpd.getLocation().list()) {
          String locName = dayLoc.getName();

          if (locName.endsWith(".avro")) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
            DataFileStream<GenericRecord> fileStream =
              new DataFileStream<>(dayLoc.getInputStream(), datumReader);
            Iterables.addAll(records, fileStream);
            fileStream.close();
          } else if (locName.endsWith(".parquet")) {
            org.apache.hadoop.fs.Path parquetFile = new org.apache.hadoop.fs.Path(dayLoc.toString());
            AvroParquetReader.Builder<GenericRecord> genericRecordBuilder = AvroParquetReader.builder(parquetFile);
            ParquetReader<GenericRecord> reader = genericRecordBuilder.build();
            GenericRecord result = reader.read();
            while (result != null) {
              records.add(result);
              result = reader.read();
            }
          }
        }
      }
      responder.send(HttpURLConnection.HTTP_OK, ByteBuffer.wrap(Bytes.toBytes(records.toString())),
                     "application/octet-stream", ImmutableMap.of());
    }
  }
}
