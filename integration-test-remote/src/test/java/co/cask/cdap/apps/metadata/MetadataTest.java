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

package co.cask.cdap.apps.metadata;

import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Negative test cases for Metadata APIs.
 */
public class MetadataTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    MetadataClient metadataClient = new MetadataClient(getClientConfig(), getRestClient());

    // test operations on nonexistent stream
    Id.Stream nonexistentStream = Id.Stream.from(TEST_NAMESPACE, "nonexistentStream");
    try {
      metadataClient.addProperties(nonexistentStream, ImmutableMap.<String, String>of());
      Assert.fail("Expected not to be able to add stream properties on a nonexistent stream.");
    } catch (IOException expected) {
      // expected
    }

    try {
      metadataClient.addTags(nonexistentStream, ImmutableList.<String>of());
      Assert.fail("Expected not to be able to add stream tags on a nonexistent stream.");
    } catch (IOException expected) {
      // expected
    }

    try {
      metadataClient.getMetadata(nonexistentStream);
      Assert.fail("Expected not to be able to get metadata for a nonexistent stream.");
    } catch (IOException expected) {
      // expected
    }

    // test operations on nonexistent service
    Id.Service nonexistentServiceId = Id.Service.from(TEST_NAMESPACE, "nonexistentApp", "nonexistentService");
    try {
      metadataClient.addProperties(nonexistentServiceId, ImmutableMap.<String, String>of());
      Assert.fail("Expected not to be able to add stream properties on a nonexistent service.");
    } catch (IOException expected) {
      // expected
    }

    try {
      metadataClient.addTags(nonexistentServiceId, ImmutableList.<String>of());
      Assert.fail("Expected not to be able to add stream tags on a nonexistent service.");
    } catch (IOException expected) {
      // expected
    }

    try {
      metadataClient.getMetadata(nonexistentServiceId);
      Assert.fail("Expected not to be able to get metadata for a nonexistent service.");
    } catch (IOException expected) {
      // expected
    }
  }
}
