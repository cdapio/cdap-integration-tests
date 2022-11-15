/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.test.suite;

import io.cdap.cdap.app.etl.KVTableWithProjectionTest;
import io.cdap.cdap.app.etl.TPFSAvroSinkSourceTest;
import io.cdap.cdap.app.etl.batch.*;
import io.cdap.cdap.app.etl.realtime.DataStreamsTest;
import io.cdap.cdap.app.etl.wrangler.WranglerServiceTest;
import io.cdap.cdap.app.etl.wrangler.WranglerTest;
import io.cdap.cdap.app.fileset.PermissionTest;
import io.cdap.cdap.app.mapreduce.readless.ReadlessIncrementTest;
import io.cdap.cdap.app.restart.HangingWorkerTest;
import io.cdap.cdap.app.serviceworker.ServiceWorkerTest;
import io.cdap.cdap.apps.ApplicationTest;
import io.cdap.cdap.apps.NamespaceTest;
import io.cdap.cdap.apps.dataset.DatasetTest;
import io.cdap.cdap.apps.fileset.FileSetTest;
import io.cdap.cdap.apps.fileset.PartitionCorrectorTest;
import io.cdap.cdap.apps.fileset.PartitionedFileSetUpdateTest;
import io.cdap.cdap.apps.metadata.ProgramMetadataTest;
import io.cdap.cdap.apps.workflow.WorkflowTest;
import io.cdap.cdap.operations.OperationalStatsTest;
import io.cdap.cdap.remote.dataset.test.RemoteCubeTest;
import io.cdap.cdap.remote.dataset.test.RemoteKeyValueTest;
import io.cdap.cdap.remote.dataset.test.RemoteTableTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Suite to represent all integration tests for CDAP.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//
// Note: all the class names below are in sorted order
//
  ApplicationTest.class,
  BatchAggregatorTest.class,
  BatchCubeSinkTest.class,
  BatchJoinerTest.class,
  DatasetTest.class,
  DataStreamsTest.class,
  DedupAggregatorTest.class,
  ETLMapReduceTest.class,
  ExcelInputReaderTest.class,
  FileSetTest.class,
  HangingWorkerTest.class,
  HivePluginTest.class,
  KVTableWithProjectionTest.class,
  NamespaceTest.class,
  NormalizeTest.class,
  OperationalStatsTest.class,
  PartitionCorrectorTest.class,
  PartitionedFileSetUpdateTest.class,
  PermissionTest.class,
  ProgramMetadataTest.class,
  ReadlessIncrementTest.class,
  RemoteCubeTest.class,
  RemoteKeyValueTest.class,
  RemoteTableTest.class,
  RowDenormalizerTest.class,
  ServiceWorkerTest.class,
  TPFSAvroSinkSourceTest.class,
  TPFSParquetSinkSourceTest.class,
  ValueMapperTest.class,
  WindowAggregationTest.class,
  WorkflowTest.class,
  WranglerServiceTest.class,
  WranglerTest.class,
  XMLReaderTest.class
})
public class AllTests {
}
