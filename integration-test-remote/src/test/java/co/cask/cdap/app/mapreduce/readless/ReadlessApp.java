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

package co.cask.cdap.app.mapreduce.readless;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one service.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A service that reads the name from the KeyValueTable and responds with 'Hello [Name]!'</li>
 * </uL>
 */
public class ReadlessApp extends AbstractApplication {

  public static final Logger LOG = LoggerFactory.getLogger(ReadlessApp.class);
  public static final String SERVICE_NAME = "CountService";
  public static final String MAPREDUCE_NAME = "LineCounter";

  @Override
  public void configure() {
    setDescription("A an app that uses datasets");
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE_NAME);
        addHandler(new CountsHandler());
      }
    });
    addMapReduce(new LineCounter());
    createDataset("counters", Table.class,
                  DatasetProperties.builder().add(Table.PROPERTY_READLESS_INCREMENT, "true").build());
  }

  private static Map<String, Long> getCounters(Table counters) {
    Row row = counters.get(new Get("counters"));
    Long increment = row.getLong("incr");
    Long mapCount = row.getLong("mapCount");
    Long reduceCount = row.getLong("reduceCount");
    return ImmutableMap.of("increments", increment == null ? -1 : increment,
                           "mapCount", mapCount == null ? -1 : mapCount,
                           "reduceCount", reduceCount == null ? -1 : reduceCount);
  }

  public static class CountsHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("get")
    public void exists(HttpServiceRequest request, HttpServiceResponder responder) throws DatasetManagementException {
      responder.sendJson(200, getCounters((Table) getContext().getDataset("counters")));
    }

    @PUT
    @Path("reset")
    public void reset(HttpServiceRequest request, HttpServiceResponder responder)
      throws DatasetManagementException {
      Table counters = getContext().getDataset("counters");
      counters.delete(new Delete("counters"));
      responder.sendStatus(200);
    }
  }

  private class LineCounter extends AbstractMapReduce {

    @Override
    protected void configure() {
      super.configure();
      setName(MAPREDUCE_NAME);
      setMapperResources(new Resources(1024));
      setReducerResources(new Resources(1024));
    }

    @Override
    protected void initialize() throws Exception {
      getContext().addOutput(Output.ofDataset("counters"));
      getContext().addInput(Input.of("mock", new InputFormatProvider() {
        @Override
        public String getInputFormatClassName() {
          return MockInputFormat.class.getName();
        }
        @Override
        public Map<String, String> getInputFormatConfiguration() {
          return Collections.emptyMap();
        }
      }));
      Job job = getContext().getHadoopJob();
      job.setMapperClass(CountMapper.class);
      job.setReducerClass(CountReducer.class);
      job.setNumReduceTasks(1);
      job.setSpeculativeExecution(false);
    }

    @Override
    public void destroy() {
      LOG.info("Final result: {} ", getCounters((Table) getContext().getDataset("counters")));
    }
  }

  public static class MockInputFormat extends InputFormat<Long, String> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      int numSplits = context.getConfiguration().getInt("num.splits", 2);
      List<InputSplit> splits = new ArrayList<>(numSplits);
      while (numSplits-- > 0) {
        splits.add(new MockSplit());
      }
      return splits;
    }
    @Override
    public RecordReader<Long, String> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new MockRecordReader();
    }
  }

  public static class MockSplit extends InputSplit implements Writable {
    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class MockRecordReader extends RecordReader<Long, String> {

    private int numLines;
    private int linesEmitted = 0;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      numLines = context.getConfiguration().getInt("num.lines", 10);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return linesEmitted++ < numLines;
    }

    @Override
    public Long getCurrentKey() throws IOException, InterruptedException {
      return (long) linesEmitted;
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
      return String.valueOf(linesEmitted);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return ((float) linesEmitted) / numLines;
    }

    @Override
    public void close() throws IOException {
    }
  }

  public static class CountMapper extends Mapper<Long, String, LongWritable, LongWritable>
    implements ProgramLifecycle<MapReduceTaskContext> {

    private static final LongWritable ONE = new LongWritable(1);
    private Table table;
    // TODO: add this back after (CDAP-6099) is fixed.
    private long mapCount = 0L;

    @Override
    public void initialize(MapReduceTaskContext context) throws Exception {
      table = context.getDataset("counters");
    }

    @Override
    protected void map(Long key, String value, Context context)
      throws IOException, InterruptedException {
      mapCount++;
      table.increment(new Increment("counters").add("incr", 1L));
      context.write(ONE, ONE);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // TODO: move this into destroy() after (CDAP-6099) is fixed.
      table.increment(new Increment("counters").add("mapCount", mapCount));
    }

    @Override
    public void destroy() {
      // TODO: move code from cleanup() to here after (CDAP-6099) is fixed.
    }
  }

  public static class CountReducer extends Reducer<LongWritable, LongWritable, byte[], Put> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      long length = Iterables.size(values);
      byte[] row = Bytes.toBytes("counters");
      context.write(row, new Put(row).add("reduceCount", Bytes.toBytes(length)));
    }
  }
}
