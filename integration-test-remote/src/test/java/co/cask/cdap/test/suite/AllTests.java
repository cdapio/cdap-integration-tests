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
import co.cask.cdap.app.etl.batch.CopybookReaderTest;
import co.cask.cdap.app.etl.batch.ETLMapReduceTest;
import co.cask.cdap.app.etl.batch.RowDenormalizerTest;
import co.cask.cdap.app.etl.batch.SparkPluginsTest;
import co.cask.cdap.app.etl.batch.ValueMapperTest;
import co.cask.cdap.app.etl.realtime.ETLWorkerTest;
import co.cask.cdap.app.etl.realtime.RealtimeCubeSinkTest;
import co.cask.cdap.apps.ApplicationTest;
import co.cask.cdap.apps.DatasetTest;
import co.cask.cdap.apps.NamespaceTest;
import co.cask.cdap.apps.NamespacedStreamTest;
import co.cask.cdap.apps.StreamTest;
import co.cask.cdap.apps.explore.ExploreTest;
import co.cask.cdap.apps.fileset.FileSetTest;
import co.cask.cdap.apps.metadata.PurchaseMetadataTest;
import co.cask.cdap.apps.purchase.PurchaseAudiTest;
import co.cask.cdap.apps.purchase.StreamSchedulerTest;
import co.cask.cdap.apps.serviceworker.ServiceWorkerTest;
import co.cask.cdap.apps.spark.sparkpagerank.SparkPageRankAppTest;
import co.cask.cdap.apps.wordcount.WordCountTest;
import co.cask.cdap.apps.workflow.WorkflowTest;
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
  ApplicationTest.class,
  BatchAggregatorTest.class,
  BatchCubeSinkTest.class,
  CopybookReaderTest.class,
  DatasetTest.class,
  ETLMapReduceTest.class,
  ETLWorkerTest.class,
  ExploreTest.class,
  FileSetTest.class,
  KVTableWithProjectionTest.class,
  NamespacedStreamTest.class,
  NamespaceTest.class,
  PurchaseAudiTest.class,
  PurchaseMetadataTest.class,
  RealtimeCubeSinkTest.class,
  RemoteCubeTest.class,
  RemoteKeyValueTest.class,
  RemoteTableTest.class,
  RowDenormalizerTest.class,
  ServiceWorkerTest.class,
  SparkPageRankAppTest.class,
  StreamSchedulerTest.class,
  StreamTest.class,
  SparkPluginsTest.class,
  StreamTPFSWithProjectionTest.class,
  ValueMapperTest.class,
  WordCountTest.class,
  WorkflowTest.class
})
public class AllTests {
}
