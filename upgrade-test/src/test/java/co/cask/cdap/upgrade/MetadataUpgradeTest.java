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

package co.cask.cdap.upgrade;

import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryBuilder;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;

import java.util.Map;
import java.util.Set;

/**
 * Upgrade tests for metadata
 */
public class MetadataUpgradeTest extends UpgradeTestBase {

  private static final Id.Application PURCHASE_APP = Id.Application.from(Id.Namespace.DEFAULT, PurchaseApp.APP_NAME);
  private static final Id.Program PURCHASE_HISTORY_BUILDER =
    Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE, PurchaseHistoryBuilder.class.getSimpleName());
  private static final Id.Stream PURCHASE_STREAM = Id.Stream.from(Id.Namespace.DEFAULT, "purchaseStream");
  private static final Id.DatasetInstance PURCHASE_STORE = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "history");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Map<String, String> APP_PROPERTIES = ImmutableMap.of("env", "prod");
  private static final Set<MetadataRecord> EXPECTED_APP_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_APP, APP_PROPERTIES, ImmutableSet.<String>of())
  );
  private static final Set<String> STREAM_TAGS = ImmutableSet.of("input");
  private static final Set<MetadataRecord> EXPECTED_STREAM_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_STREAM, EMPTY_PROPERTIES, STREAM_TAGS)
  );
  private static final Set<String> MR_TAGS = ImmutableSet.of("processing");
  private static final Set<MetadataRecord> EXPECTED_MR_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_HISTORY_BUILDER, EMPTY_PROPERTIES, MR_TAGS)
  );
  private static final Set<String> DS_TAGS = ImmutableSet.of("output");
  private static final Set<MetadataRecord> EXPECTED_DS_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_STORE, EMPTY_PROPERTIES, DS_TAGS)
  );

  private final MetadataClient metadataClient;

  public MetadataUpgradeTest() {
    this.metadataClient = new MetadataClient(getClientConfig(), getRestClient());
  }

  @Override
  protected void preStage() throws Exception {
    deployApplication(PurchaseApp.class);
    // Add some metadata
    metadataClient.addProperties(PURCHASE_APP, APP_PROPERTIES);
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP));

    metadataClient.addTags(PURCHASE_STREAM, STREAM_TAGS);
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM));

    metadataClient.addTags(PURCHASE_HISTORY_BUILDER, MR_TAGS);
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER));

    metadataClient.addTags(PURCHASE_STORE, DS_TAGS);
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(PURCHASE_STORE));
  }

  @Override
  protected void postStage() throws Exception {
    Assert.assertTrue("PurchaseApp must exist after upgrade.", getApplicationClient().exists(PURCHASE_APP));
    // verify user metadata added prior to upgrade
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(PURCHASE_STORE, MetadataScope.USER));
    // verify search using user metadata added prior to upgrade
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_APP)),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "env:prod", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_STREAM)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "input", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "process*", MetadataSearchTargetType.PROGRAM)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_STORE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "output", MetadataSearchTargetType.ALL)
    );

    // assert that system metadata is empty
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(PURCHASE_APP, MetadataScope.SYSTEM)),
      metadataClient.getMetadata(PURCHASE_APP, MetadataScope.SYSTEM)
    );
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(PURCHASE_STREAM, MetadataScope.SYSTEM)),
      metadataClient.getMetadata(PURCHASE_STREAM, MetadataScope.SYSTEM)
    );
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(PURCHASE_HISTORY_BUILDER, MetadataScope.SYSTEM)),
      metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER, MetadataScope.SYSTEM)
    );
    Assert.assertEquals(
      ImmutableSet.of(new MetadataRecord(PURCHASE_STORE, MetadataScope.SYSTEM)),
      metadataClient.getMetadata(PURCHASE_STORE, MetadataScope.SYSTEM)
    );
  }
}
