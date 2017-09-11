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

package co.cask.cdap.longrunning.txprune;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 */
@SuppressWarnings("WeakerAccess")
public class InvalidTxGeneratorApp extends AbstractApplication {
  public static final String STREAM = "invalid-tx-data";
  public static final String DATASET = "invalid-tx-kv";
  public static final String EXCEPTION_STRING = "exception";
  public static final String APP_NAME = "InvalidTxGeneratorApp";

  @Override
  public void configure() {
    setName(APP_NAME);
    addMapReduce(new InvalidMapReduce());
    addStream(STREAM);
    createDataset(DATASET, KeyValueTable.class);
  }

  @SuppressWarnings("WeakerAccess")
  public static class InvalidMapReduce extends AbstractMapReduce {
    public static final String MR_NAME = "InvalidMapReduce";

    @Override
    protected void configure() {
      setName(MR_NAME);
    }

    @Override
    protected void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      job.setMapperClass(InputReader.class);
      job.setNumReduceTasks(0);
//    This is so that the bufferred data gets flushed to the dataset before the map reduce job fails.
//    The default value is 10k - hence the data may not be flushed if the job fails before it ingests 10k events.
      job.getConfiguration().setInt("c.mapper.flush.freq", 10);
      context.addInput(Input.ofStream(STREAM));
      context.addOutput(Output.ofDataset(DATASET));
    }

    public static class InputReader extends Mapper<LongWritable, Text, byte[],  byte[]> {
      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (EXCEPTION_STRING.equals(value.toString())) {
          throw new IOException("Throwing an exception to create an invalid transaction!");
        }
        context.write(value.getBytes(), value.getBytes());
      }
    }
  }
}
