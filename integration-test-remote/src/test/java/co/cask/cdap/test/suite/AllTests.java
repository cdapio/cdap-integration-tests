/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.test.suite;

import co.cask.cdap.app.etl.KVTableWithProjectionTest;
import co.cask.cdap.app.etl.StreamTPFSWithProjectionTest;
import co.cask.cdap.app.etl.batch.BatchAggregatorTest;
import co.cask.cdap.app.etl.batch.BatchCubeSinkTest;
import co.cask.cdap.app.etl.batch.BatchJoinerTest;
import co.cask.cdap.app.etl.batch.ETLMapReduceTest;
import co.cask.cdap.app.etl.batch.ExcelInputReaderTest;
import co.cask.cdap.app.etl.batch.NormalizeTest;
import co.cask.cdap.app.etl.batch.RowDenormalizerTest;
import co.cask.cdap.app.etl.batch.TPFSSinkSourceTest;
import co.cask.cdap.app.etl.batch.ValueMapperTest;
import co.cask.cdap.app.etl.batch.XMLReaderTest;
import co.cask.cdap.app.etl.realtime.DataStreamsTest;
import co.cask.cdap.app.etl.wrangler.WranglerServiceTest;
import co.cask.cdap.app.etl.wrangler.WranglerTest;
import co.cask.cdap.app.fileset.PermissionTest;
import co.cask.cdap.app.mapreduce.readless.ReadlessIncrementTest;
import co.cask.cdap.app.restart.HangingWorkerTest;
import co.cask.cdap.app.serviceworker.ServiceWorkerTest;
import co.cask.cdap.apps.ApplicationTest;
import co.cask.cdap.apps.ApplicationVersionTest;
import co.cask.cdap.apps.DatasetTest;
import co.cask.cdap.apps.NamespaceTest;
import co.cask.cdap.apps.NamespacedStreamTest;
import co.cask.cdap.apps.StreamTest;
import co.cask.cdap.apps.explore.ExploreTest;
import co.cask.cdap.apps.fileset.FileSetTest;
import co.cask.cdap.apps.fileset.PartitionCorrectorTest;
import co.cask.cdap.apps.fileset.PartitionedFileSetUpdateTest;
import co.cask.cdap.apps.metadata.PurchaseMetadataTest;
import co.cask.cdap.apps.purchase.PurchaseAudiTest;
import co.cask.cdap.apps.spark.sparkpagerank.SparkPageRankAppTest;
import co.cask.cdap.apps.tracker.TrackerTest;
import co.cask.cdap.apps.transaction.TransactionTimeoutTest;
import co.cask.cdap.apps.wordcount.WordCountTest;
import co.cask.cdap.apps.workflow.WorkflowTest;
import co.cask.cdap.operations.OperationalStatsTest;
import co.cask.cdap.remote.dataset.test.RemoteCubeTest;
import co.cask.cdap.remote.dataset.test.RemoteKeyValueTest;
import co.cask.cdap.remote.dataset.test.RemoteTableTest;
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
  ApplicationVersionTest.class,
  BatchAggregatorTest.class,
  BatchCubeSinkTest.class,
  BatchJoinerTest.class,
  DatasetTest.class,
  DataStreamsTest.class,
  ETLMapReduceTest.class,
  ExcelInputReaderTest.class,
  ExploreTest.class,
  FileSetTest.class,
  HangingWorkerTest.class,
  KVTableWithProjectionTest.class,
  NamespacedStreamTest.class,
  NamespaceTest.class,
  NormalizeTest.class,
  OperationalStatsTest.class,
  PartitionCorrectorTest.class,
  PartitionedFileSetUpdateTest.class,
  PermissionTest.class,
  PurchaseAudiTest.class,
  PurchaseMetadataTest.class,
  ReadlessIncrementTest.class,
  RemoteCubeTest.class,
  RemoteKeyValueTest.class,
  RemoteTableTest.class,
  RowDenormalizerTest.class,
  ServiceWorkerTest.class,
  SparkPageRankAppTest.class,
  StreamTest.class,
  StreamTPFSWithProjectionTest.class,
  TPFSSinkSourceTest.class,
  TrackerTest.class,
  TransactionTimeoutTest.class,
  ValueMapperTest.class,
  WordCountTest.class,
  WorkflowTest.class,
  WranglerServiceTest.class,
  WranglerTest.class,
  XMLReaderTest.class
})
public class AllTests {
}
