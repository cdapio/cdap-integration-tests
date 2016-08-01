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

import co.cask.cdap.api.dataset.lib.cube.TimeValue;

import co.cask.cdap.proto.audit.AuditMessage;

import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.AuditLogResponse;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.TrackerMeterResult;
import co.cask.tracker.entity.ValidateTagsResult;


import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test various methods of preferred Tags.
 */
public class TrackerTest extends TrackerTestBase {
  private static final String TEST_JSON_TAGS = "[\"tag1\",\"tag2\",\"tag3\",\"ta*4\"]";
  private static final String DEMOTE_TAGS = "[\"tag1\"]";
  private static final String DELETE_TAGS = "tag2";
  private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123";
  private static final int MAX_TAG_LENGTH = 50;
  private static final int TAGNUM = 10;


  @Test
  public void test() throws Exception {
    enableTracker();

    List<AuditMessage> testData = generateTestData();
    sendTestAuditMessages(testData);

    waitforProcessed(testData.size());

    AuditLogResponse auditLogResponse = getAuditLog();
    Assert.assertNotEquals(0, auditLogResponse.getTotalResults());

    List<TopDatasetsResult> topDatasetsResults = getTopNDatasets();
    Assert.assertEquals(6, topDatasetsResults.size());
    long testTotal1 = topDatasetsResults.get(0).getRead() + topDatasetsResults.get(0).getWrite();
    long testTotal2 = topDatasetsResults.get(1).getRead() + topDatasetsResults.get(1).getWrite();
    Assert.assertEquals(true, testTotal1 > testTotal2);

    List<TopProgramsResult> topProgramsResults = getTopNPrograms();
    Assert.assertEquals(5, topProgramsResults.size());
    Assert.assertEquals(true, topProgramsResults.get(0).getValue() > topProgramsResults.get(1).getValue());
    Assert.assertEquals("service", topProgramsResults.get(0).getProgramType());
    Assert.assertEquals("b", topProgramsResults.get(0).getApplication());

    List<TopApplicationsResult> topApplicationsResults = getTopNApplication();
    Assert.assertEquals(4, topApplicationsResults.size());
    Assert.assertEquals(true, topApplicationsResults.get(0).getValue() > topApplicationsResults.get(1).getValue());

    Map<String, Long> timeSinceResult = getTimeSince();
    Assert.assertEquals(2, timeSinceResult.size());

    AuditHistogramResult auditGlobalHistogramResult = getGlobalAuditLogHistogram();
    Collection<TimeValue> globalValueResults = auditGlobalHistogramResult.getResults();
    int gtotal = 0;
    for (TimeValue t: globalValueResults) {
      gtotal += t.getValue();
    }
    Assert.assertEquals(17, gtotal);

    AuditHistogramResult auditSpecificHistogramResult = getSpecificAuditLogHistogram();
    Collection<TimeValue> specificValueResults = auditSpecificHistogramResult.getResults();
    int stotal = 0;
    for (TimeValue t: specificValueResults) {
      stotal += t.getValue();
    }
    Assert.assertEquals(5, stotal);

    AuditHistogramResult auditHistogramResultHour = getResolutionBucket("now-6d");
    Assert.assertEquals("HOUR", auditHistogramResultHour.getBucketInterval());

    AuditHistogramResult auditHistogramResultDay = getResolutionBucket("now-8d");
    Assert.assertEquals("DAY", auditHistogramResultDay.getBucketInterval());

    AuditHistogramResult datasetFilter = getDatasetFilter();
    Assert.assertEquals(0, datasetFilter.getResults().size());

    AuditHistogramResult kafkaFilter = getKafkaFilter();
    Assert.assertEquals(0, kafkaFilter.getResults().size());

    List<TopProgramsResult> programsFilter = getProgramFilter();
    Assert.assertEquals(0, programsFilter.size());

    promoteTags(TEST_JSON_TAGS);

    TagsResult tagsResult = getPreferredTags();
    Assert.assertEquals(3, tagsResult.getPreferredSize());

    demoteTags(DEMOTE_TAGS);
    TagsResult tagsAfterDemote = getPreferredTags();
    Assert.assertEquals(2, tagsAfterDemote.getPreferredSize());

    deleteTags(DELETE_TAGS);
    TagsResult tagsAfterDelete = getPreferredTags();
    Assert.assertEquals(1, tagsAfterDelete.getPreferredSize());

    ValidateTagsResult tagsAfterValidate = validateTags(TEST_JSON_TAGS);
    Assert.assertEquals(3, tagsAfterValidate.getValid());
    Assert.assertEquals(1, tagsAfterValidate.getInvalid());

    Set<String> tagSet = generateStringList(MAX_TAG_LENGTH, CHARACTERS, TAGNUM);
    addEntityTags(tagSet, "dataset", "_auditTagsTable");
    TagsResult dataSetUserTags = getEntityTags("dataset", "_auditTagsTable");
    Assert.assertEquals(tagSet.size(), dataSetUserTags.getUserSize());
    deleteEntityTags(tagSet, "dataset", "_auditTagsTable");
    Assert.assertEquals(tagSet.size(), dataSetUserTags.getUserSize());

    TrackerMeterResult truthMeter = getTruthMeter();
    Assert.assertEquals(3, truthMeter.getDatasets().size());
    Assert.assertEquals(1, truthMeter.getStreams().size());

    TrackerMeterResult timeRank = getTimeRank();
    Assert.assertEquals(timeRank.getDatasets().get("ds8"), timeRank.getDatasets().get("ds9"));

    TrackerMeterResult invalidName = getInvalidName();
    for (Map.Entry<String, Integer> entry : invalidName.getDatasets().entrySet()) {
      Assert.assertEquals(0, (int) entry.getValue());
    }
    for (Map.Entry<String, Integer> entry : invalidName.getStreams().entrySet()) {
      Assert.assertEquals(0, (int) entry.getValue());
    }

  }
}
