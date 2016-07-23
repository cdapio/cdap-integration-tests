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

import co.cask.cdap.api.app.AbstractApplication;

import co.cask.cdap.api.dataset.DatasetProperties;

import co.cask.tracker.TrackerApp;
import co.cask.tracker.TrackerService;
import co.cask.tracker.config.TrackerAppConfig;
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.entity.AuditMetricsCube;
import co.cask.tracker.entity.AuditTagsTable;
import co.cask.tracker.entity.LatestEntityTable;


import java.util.concurrent.TimeUnit;
/**
 * temp app for test
 */
public class TestTrackerApp extends AbstractApplication<TrackerAppConfig> {
  public static final String STREAM_NAME = "testStream";
  public static final String FLOWLET_NAME = "auditLogPublisher";
  @Override
  public void configure() {
    setName("TestTrackerApp");
    setDescription("A temp app to test the Tracker App");
    addStream(STREAM_NAME);
    createDataset(TrackerApp.AUDIT_LOG_DATASET_NAME, AuditLogTable.class);
    String resolutions = String.format("%s,%s,%s",
                                       TimeUnit.HOURS.toSeconds(1L),
                                       TimeUnit.DAYS.toSeconds(1L),
                                       TimeUnit.DAYS.toSeconds(365L));
    createDataset(TrackerApp.AUDIT_METRICS_DATASET_NAME,
                  AuditMetricsCube.class,
                  DatasetProperties.builder()
                    .add("dataset.cube.resolutions", resolutions)
                    .add("dataset.cube.aggregation.agg1.dimensions",
                         "namespace,entity_type,entity_name,audit_type")
                    .add("dataset.cube.aggregation.agg2.dimensions",
                         "namespace,entity_type,entity_name,audit_type,program_name,app_name,program_type")
                    .build());
    createDataset(TrackerApp.ENTITY_LATEST_TIMESTAMP_DATASET_NAME, LatestEntityTable.class);
    createDataset(TrackerApp.AUDIT_TAGS_DATASET_NAME, AuditTagsTable.class);
    addFlow(new StreamToAuditLogFlow());
    addService(new TrackerService(getConfig()));
  }
}
