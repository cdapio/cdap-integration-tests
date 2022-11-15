/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.app.etl.gcp.DLPTest;
import io.cdap.cdap.app.etl.gcp.GCSTest;
import io.cdap.cdap.app.etl.gcp.GoogleBigQueryTest;
import io.cdap.cdap.app.etl.gcp.GoogleCloudBigtableTest;
import io.cdap.cdap.app.etl.gcp.GoogleCloudDatastoreTest;
import io.cdap.cdap.app.etl.gcp.GoogleCloudSpannerTest;
import io.cdap.cdap.app.etl.gcp.PubSubTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for GCP related tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//  GCSTest.class,
//  GoogleBigQueryTest.class,
//  GoogleCloudBigtableTest.class,
//  GoogleCloudDatastoreTest.class,
  GoogleCloudSpannerTest.class,
  PubSubTest.class,
  DLPTest.class
})
public class GCPSuite {

}
