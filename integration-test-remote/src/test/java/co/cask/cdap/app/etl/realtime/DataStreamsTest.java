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

package co.cask.cdap.app.etl.realtime;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.etl.ETLTestBase;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.suite.category.CDH51Incompatible;
import co.cask.cdap.test.suite.category.CDH52Incompatible;
import co.cask.cdap.test.suite.category.CDH53Incompatible;
import co.cask.cdap.test.suite.category.CDH54Incompatible;
import co.cask.cdap.test.suite.category.HDP20Incompatible;
import co.cask.cdap.test.suite.category.HDP21Incompatible;
import co.cask.cdap.test.suite.category.HDP22Incompatible;
import co.cask.cdap.test.suite.category.MapR5Incompatible;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests for DataStreams app.
 */
public class DataStreamsTest extends ETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamsTest.class);

  // DataStreams are based on Spark runtime, so marking incompatible for all Hadoop versions that don't support Spark
  @Category({
    // We don't support spark on these distros
    HDP20Incompatible.class,
    HDP21Incompatible.class,
    CDH51Incompatible.class,
    CDH52Incompatible.class,
    // (HYDRATOR-1134) All spark plugins fail to load on CDH 5.3, due to one of them not not being able to load on
    // CDH 5.3. Once this JIRA is resolved, we should look at all the places we have used CDH53Incompatible, and see if
    // they're still incompatible.
    CDH53Incompatible.class,
    // (HYDRATOR-1135) Mark HDP 2.2 and CDH 5.4 incompatible at least until we resolve this JIRA.
    HDP22Incompatible.class,
    CDH54Incompatible.class,
    // Currently, coopr doesn't provision MapR cluster with Spark. Enable this test once COOK-108 is fixed
    MapR5Incompatible.class // MapR5x category is used for all MapR version
  })
  @Test
  public void testKafkaAggregatorTable() throws Exception {
    String hostname = getClientConfig().getConnectionConfig().getHostname();
    String topic = UUID.randomUUID().toString();

    Schema purchaseSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("num", Schema.of(Schema.Type.LONG)));

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("referenceName", topic);
    sourceProperties.put("brokers", hostname + ":9092");
    sourceProperties.put("topic", topic);
    sourceProperties.put("defaultInitialOffset", "-2");
    sourceProperties.put("schema", purchaseSchema.toString());
    sourceProperties.put("format", "csv");

    Schema userStatsSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("itemsPurchased", Schema.of(Schema.Type.LONG)));

    String outputTableName = "streamingUserStats";
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("name", outputTableName);
    sinkProperties.put("schema", userStatsSchema.toString());
    sinkProperties.put("schema.row.field", "user");

    Map<String, String> aggProperties = new HashMap<>();
    aggProperties.put("aggregates", "itemsPurchased:sum(num)");
    aggProperties.put("groupByFields", "user");

    DataStreamsConfig config = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", new ETLPlugin("Kafka", StreamingSource.PLUGIN_TYPE, sourceProperties, null)))
      .addStage(new ETLStage("sink", new ETLPlugin("Table", BatchSink.PLUGIN_TYPE, sinkProperties, null)))
      .addStage(new ETLStage("agg", new ETLPlugin("GroupByAggregate", BatchAggregator.PLUGIN_TYPE,
                                                  aggProperties, null)))
      .addConnection("source", "agg")
      .addConnection("agg", "sink")
      .setBatchInterval("60s")
      .build();

    ApplicationId appId = TEST_NAMESPACE.app("kafkaStreaming");
    AppRequest<DataStreamsConfig> appRequest = getStreamingAppRequest(config);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    try (KafkaProducer<String, String> kafkaProducer = getKafkaProducer()) {
      List<Future<RecordMetadata>> futures = new ArrayList<>();
      futures.add(kafkaProducer.send(new ProducerRecord<String, String>(topic, "samuel,wallet,1")));
      futures.add(kafkaProducer.send(new ProducerRecord<String, String>(topic, "samuel,shirt,2")));
      futures.add(kafkaProducer.send(new ProducerRecord<String, String>(topic, "samuel,egg,4")));
      for (Future<RecordMetadata> future : futures) {
        future.get(1, TimeUnit.MINUTES);
      }
      LOG.info("Done sending events to kafka.");
    }

    SparkManager sparkManager = appManager.getSparkManager("DataStreamsSparkStreaming");
    // spark client occasionally is restarted by YARN for going OOM (with 512mb memory)
    sparkManager.start(ImmutableMap.of("task.client.system.resources.memory", "768"));
    sparkManager.waitForStatus(true, PROGRAM_START_STOP_TIMEOUT_SECONDS, 1);

    final DataSetManager<Table> tableManager = getTableDataset(outputTableName);
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Table table = tableManager.get();
        byte[] val = table.get(Bytes.toBytes("samuel"), Bytes.toBytes("itemsPurchased"));
        return val != null && Bytes.toLong(val) == 7L;
      }
    }, 5, TimeUnit.MINUTES, 5, TimeUnit.SECONDS);

    sparkManager.stop();
    // Change wait time to PROGRAM_START_STOP_TIMEOUT_SECONDS once CDAP-7724 is fixed
    sparkManager.waitForStatus(false, 5 * 60, 1);
  }

  private KafkaProducer<String, String> getKafkaProducer() {
    String hostname = getClientConfig().getConnectionConfig().getHostname();
    Properties props = new Properties();
    props.put("bootstrap.servers", hostname + ":9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("request.required.acks", "1");
    return new KafkaProducer<>(props);
  }
}
