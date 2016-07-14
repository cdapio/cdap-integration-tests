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

package co.cask.cdap.apps.tracker;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import co.cask.cdap.api.dataset.lib.cube.TimeValue;

import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;

import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.TrackerMeterRequest;
import co.cask.tracker.entity.TrackerMeterResult;
import co.cask.tracker.entity.ValidateTagsResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Test various methods of preferred Tags.
 */
public class PreferredTagsTest extends TrackerTestBase{


  private static final String TEST_JSON_TAGS = "[\"tag1\",\"tag2\",\"tag3\",\"ta*4\"]";
  private static final String DEMOTE_TAGS = "[\"tag1\"]";
  private static final String DELETE_TAGS = "[\"tag2\"]";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private String testTruthMeter = "";

  @Test
  public void test() throws Exception {
    enableTracker();

    List<AuditMessage> testData = generateTestData();
    sendTestAuditMessages(testData);

    waitforProcessed(testData.size());

    promoteTags(TEST_JSON_TAGS);

    TagsResult tagsResult = getPreferredTags();
    Assert.assertEquals(3, tagsResult.getPreferred());

    demoteTags(DEMOTE_TAGS);
    TagsResult tagsAfterDemote = getPreferredTags();
    Assert.assertEquals(2, tagsAfterDemote.getPreferred());

    demoteTags(DELETE_TAGS);
    TagsResult tagsAfterDelete = getPreferredTags();
    Assert.assertEquals(1, tagsAfterDelete.getPreferred());

    ValidateTagsResult tagsAfterValidate = validateTags(TEST_JSON_TAGS);
    Assert.assertEquals(3, tagsAfterValidate.getValid());
    Assert.assertEquals(1, tagsAfterValidate.getInvalid());

    List<TopDatasetsResult> topDatasetsResults = getTopNDatasets();
    Assert.assertEquals(4, topDatasetsResults.size());

    List<TopProgramsResult> topProgramsResults = getTopNPrograms();
    Assert.assertEquals(5, topProgramsResults.size());

    List<TopApplicationsResult> topApplicationsResults = getTopNApplication();
    Assert.assertEquals(4, topApplicationsResults.size());

    Map<String, Long> timeSinceResult = getTimeSince();
    Assert.assertEquals(2, timeSinceResult.size());

    AuditHistogramResult auditHistogramResult = getAuditLogHistogram();
    Collection<TimeValue> valueResults = auditHistogramResult.getResults();
    int total = 0;
    for(TimeValue t: valueResults) {
      total +=t.getValue();
    }
    Assert.assertEquals(14, total);

    initializeTruthMeterInput();
    TrackerMeterResult trackerMeterResult = getTrackerMeter(testTruthMeter);
    Assert.assertEquals(3, trackerMeterResult.getDatasets().size());
    Assert.assertEquals(1, trackerMeterResult.getStreams().size());
  }


  private void initializeTruthMeterInput() {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds1");
    datasets.add("ds6");
    datasets.add("ds3");
    streams.add("strm123");
    testTruthMeter = GSON.toJson(new TrackerMeterRequest(datasets, streams));
  }

  private List<AuditMessage> generateTestData() {
    List<AuditMessage> testData = new ArrayList<>();
    testData.add(new AuditMessage(1456956659461L,
                                  NamespaceId.DEFAULT.stream("stream1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program_run:ns1.app2.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659469L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    String metadataPayload = "{ \"previous\": { \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, " +
      "\"tags\": [ \"ut1\", \"ut2\" ] }, \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, " +
      "\"additions\": { \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, " +
      "\"deletions\": { \"USER\": { \"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } }";
    MetadataPayload payload = GSON.fromJson(metadataPayload, MetadataPayload.class);
    testData.add(new AuditMessage(1456956659470L,
                                  EntityId.fromString("application:default.app1"),
                                  "user1",
                                  AuditType.METADATA_CHANGE,
                                  payload)
    );
    testData.add(new AuditMessage(1456956659471L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659472L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659473L,
                                  EntityId.fromString("dataset:default.ds6"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659468L,
                                  NamespaceId.DEFAULT.stream("strm123"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program_run:ns1.app1.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659460L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659502L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659500L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659504L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.UNKNOWN,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659505L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659506L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.a.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659507L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    return testData;
  }
}
