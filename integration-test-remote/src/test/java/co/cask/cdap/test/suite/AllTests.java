/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.app.etl.batch.BatchCubeSinkTest;
import co.cask.cdap.app.etl.batch.ETLMapReduceTest;
import co.cask.cdap.app.etl.realtime.ETLWorkerTest;
import co.cask.cdap.app.etl.realtime.RealtimeCubeSinkTest;
import co.cask.cdap.apps.ApplicationTest;
import co.cask.cdap.apps.DatasetTest;
import co.cask.cdap.apps.NamespaceTest;
import co.cask.cdap.apps.NamespacedStreamTest;
import co.cask.cdap.apps.StreamTest;
import co.cask.cdap.apps.explore.ExploreTest;
import co.cask.cdap.apps.fileset.FileSetTest;
import co.cask.cdap.apps.metadata.PurchaseLineageTest;
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
  BatchCubeSinkTest.class,
  ETLMapReduceTest.class,
  KVTableWithProjectionTest.class,
  ETLWorkerTest.class,
  RealtimeCubeSinkTest.class,
  StreamTPFSWithProjectionTest.class,
  ApplicationTest.class,
  DatasetTest.class,
  ExploreTest.class,
  FileSetTest.class,
  PurchaseLineageTest.class,
  NamespacedStreamTest.class,
  NamespaceTest.class,
  PurchaseAudiTest.class,
  StreamSchedulerTest.class,
  ServiceWorkerTest.class,
  SparkPageRankAppTest.class,
  StreamTest.class,
  WordCountTest.class,
  WorkflowTest.class,
  RemoteCubeTest.class,
  RemoteKeyValueTest.class,
  RemoteTableTest.class
})
public class AllTests {
}
