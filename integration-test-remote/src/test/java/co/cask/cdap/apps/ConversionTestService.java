package co.cask.cdap.apps;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

public class ConversionTestService extends AbstractService {

  @Override
  protected void configure() {
    setName("ConversionTestService");
    setDescription("A Service to read the TPFS files");
    setInstances(1);
    addHandler(new TPFSHandler());
  }

  public static class TPFSHandler extends AbstractHttpServiceHandler {

    @UseDataSet("temp")
    private TimePartitionedFileSet temp;

    @GET
    @Path("tpfs")
    public void readTPFS(HttpServiceRequest request, HttpServiceResponder responder,
                         @QueryParam("time") long time) throws IOException {

      Set<TimePartitionDetail> timePartitionDetailSet = temp.getPartitionsByTime(time, System.currentTimeMillis()
        + TimeUnit.MINUTES.toMillis(1));
      Schema schema = Schema.recordOf(
        "streamEvent",
        Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("num", Schema.of(Schema.Type.INT)),
        Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());

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
      responder.send(200, ByteBuffer.wrap(Bytes.toBytes(records.toString())), "application/octet-stream",
                     ImmutableMultimap.<String, String>of());
    }
  }
}
