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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryBuilder;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Assert;

import java.util.HashSet;
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
  private static final Id.Stream.View PURCHASE_VIEW = Id.Stream.View.from(PURCHASE_STREAM,
                                                                          PURCHASE_STREAM.getId() + "View");
  private static final Id.DatasetInstance HISTORY = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "history");
  private static final Id.DatasetInstance FREQUENT_CUSTOMERS = Id.DatasetInstance.from(Id.Namespace.DEFAULT,
                                                                                       "frequentCustomers");
  private static final Id.DatasetInstance USER_PROFILES = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "userProfiles");
  private static final Id.DatasetInstance PURCHASES = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "purchases");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final Map<String, String> APP_PROPERTIES = ImmutableMap.of("env", "prod");
  private static final Set<MetadataRecord> EXPECTED_APP_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_APP, MetadataScope.USER, APP_PROPERTIES, ImmutableSet.<String>of())
  );
  private static final Set<String> STREAM_TAGS = ImmutableSet.of("input");
  private static final Set<MetadataRecord> EXPECTED_STREAM_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_STREAM, MetadataScope.USER, EMPTY_PROPERTIES, STREAM_TAGS)
  );
  private static final Set<String> MR_TAGS = ImmutableSet.of("processing");
  private static final Set<MetadataRecord> EXPECTED_MR_METADATA = ImmutableSet.of(
    new MetadataRecord(PURCHASE_HISTORY_BUILDER, MetadataScope.USER, EMPTY_PROPERTIES, MR_TAGS)
  );
  private static final Set<String> DS_TAGS = ImmutableSet.of("output");
  private static final Set<MetadataRecord> EXPECTED_DS_METADATA = ImmutableSet.of(
    new MetadataRecord(HISTORY, MetadataScope.USER, EMPTY_PROPERTIES, DS_TAGS)
  );
  private static final String PURCHASE_VIEW_FIELD = "purchaseViewBody";
  private static Predicate<MetadataSearchResultRecord> purchaseAppPredicate;

  private final MetadataClient metadataClient;

  public MetadataUpgradeTest() {
    this.metadataClient = new MetadataClient(getClientConfig(), getRestClient());
  }

  @Override
  protected void preStage() throws Exception {
    // deploy an application
    deployApplication(PurchaseApp.class);

    // create a view
    Schema viewSchema = Schema.recordOf("record", Schema.Field.of(PURCHASE_VIEW_FIELD,
                                                                  Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StreamViewClient viewClient = new StreamViewClient(getClientConfig(), getRestClient());
    viewClient.createOrUpdate(PURCHASE_VIEW, new ViewSpecification(new FormatSpecification("format", viewSchema)));

    // Add some user metadata
    metadataClient.addProperties(PURCHASE_APP, APP_PROPERTIES);
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP, MetadataScope.USER));

    metadataClient.addTags(PURCHASE_STREAM, STREAM_TAGS);
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM, MetadataScope.USER));

    metadataClient.addTags(PURCHASE_HISTORY_BUILDER, MR_TAGS);
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER, MetadataScope.USER));

    metadataClient.addTags(HISTORY, DS_TAGS);
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(HISTORY, MetadataScope.USER));

    // there should be system metadata records for these entities
    verifySystemMetadata(PURCHASE_APP, true, true);
    verifySystemMetadata(HISTORY, true, true);
    // currently we don't have any properties for programs
    verifySystemMetadata(PURCHASE_HISTORY_BUILDER, false, true);
    verifySystemMetadata(PURCHASE_STREAM, true, true);
  }

  @Override
  protected void postStage() throws Exception {
    Assert.assertTrue("PurchaseApp must exist after upgrade.", getApplicationClient().exists(PURCHASE_APP));
    // verify user metadata added prior to upgrade
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER, MetadataScope.USER));
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(HISTORY, MetadataScope.USER));
    // verify search using user metadata added prior to upgrade
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_APP)),
      searchMetadata(Id.Namespace.DEFAULT, "env:prod", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_STREAM)
      ),
      searchMetadata(Id.Namespace.DEFAULT, "input", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER)
      ),
      searchMetadata(Id.Namespace.DEFAULT, "process*", MetadataSearchTargetType.PROGRAM)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(HISTORY)
      ),
      searchMetadata(Id.Namespace.DEFAULT, "output", MetadataSearchTargetType.ALL)
    );

    // there should be system metadata records for these entities
    verifySystemMetadata(PURCHASE_APP, true, true);
    verifySystemMetadata(HISTORY, true, true);
    // currently we don't have any properties for programs
    verifySystemMetadata(PURCHASE_HISTORY_BUILDER, false, true);
    verifySystemMetadata(PURCHASE_STREAM, true, true);

    // makes some searches: this should get system entities such as dataset, artifacts, flow, services, programs
    Set<MetadataSearchResultRecord> searchResults = filterNonPurchaseEntities(
      searchMetadata(Id.Namespace.DEFAULT, "explore", MetadataSearchTargetType.ALL));
    // 4 = dataset: frequentCustomers + dataset: userProfiles + dataset: purchases + dataset: history
    Assert.assertEquals(4, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(Id.Namespace.DEFAULT, "batch",
                                                             MetadataSearchTargetType.ALL));
    // 6 = dataset: frequentCustomers + dataset: userProfiles +
    // dataset: purchases + dataset: history +  workflow: PurchaseHistoryWorkflow + mapreduce: PurchaseHistoryBuilder
    Assert.assertEquals(6, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(Id.Namespace.DEFAULT, "realtime",
                                                             MetadataSearchTargetType.ALL));
    // 4 = service: CatalogLookup + service: UserProfileService + service: PurchaseHistoryService + flow: PurchaseFlow
    Assert.assertEquals(4, searchResults.size());

    // system metadata for app check
    searchResults = searchMetadata(Id.Namespace.DEFAULT, PURCHASE_APP.getId(),
                                   MetadataSearchTargetType.ALL);
    Assert.assertEquals(1, searchResults.size());

    // system metadata for stream check
    searchResults = searchMetadata(Id.Namespace.DEFAULT, PURCHASE_STREAM.getId(),
                                   MetadataSearchTargetType.ALL);
    // 3 = stream: purchaseStream + app: PurchaseHistory + view: purchaseStreamView
    Assert.assertEquals(3, searchResults.size());

    // perform schema searches
    searchResults = filterNonPurchaseEntities(searchMetadata(Id.Namespace.DEFAULT, "price",
                                                             MetadataSearchTargetType.ALL));
    // 2 = dataset: purchases + dataset: history
    Assert.assertEquals(2, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(Id.Namespace.DEFAULT, "lastname:string",
                                                             MetadataSearchTargetType.ALL));
    // 1 =  dataset: history
    Assert.assertEquals(1, searchResults.size());

    // search for view schema
    searchResults = searchMetadata(Id.Namespace.DEFAULT, PURCHASE_VIEW_FIELD,
                                   MetadataSearchTargetType.ALL);
    Assert.assertEquals(1, searchResults.size());
  }

  private void verifySystemMetadata(Id.NamespacedId id, boolean checkProperties, boolean checkTags) throws Exception {
    Set<MetadataRecord> metadataRecords = metadataClient.getMetadata(id, MetadataScope.SYSTEM);
    Assert.assertEquals(1, metadataRecords.size());
    MetadataRecord metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.SYSTEM, metadata.getScope());
    if (checkProperties) {
      Assert.assertTrue(metadata.getProperties().size() != 0);
    }
    if (checkTags) {
      Assert.assertTrue(metadata.getTags().size() != 0);
    }
  }

  private Set<MetadataSearchResultRecord> filterNonPurchaseEntities(Set<MetadataSearchResultRecord> results) {
    return ImmutableSet.copyOf(Iterables.filter(results, getPurchaseAppPredicate()));
  }

  private Predicate<MetadataSearchResultRecord> getPurchaseAppPredicate() {
    if (purchaseAppPredicate == null) {
      purchaseAppPredicate = new Predicate<MetadataSearchResultRecord>() {
        @Override
        public boolean apply(MetadataSearchResultRecord input) {
          Id.NamespacedId entityId = input.getEntityId();
          if (entityId instanceof Id.DatasetInstance) {
            return ImmutableSet.of(HISTORY, USER_PROFILES, PURCHASES, FREQUENT_CUSTOMERS).contains(entityId);
          } else if (entityId instanceof Id.Stream.View) {
            return PURCHASE_VIEW.equals(entityId);
          } else if (entityId instanceof Id.Stream) {
            return PURCHASE_STREAM.equals(entityId);
          } else if (entityId instanceof Id.Application) {
            return PURCHASE_APP.equals(entityId);
          } else if (entityId instanceof Id.Program) {
            return PURCHASE_APP.equals(((Id.Program) entityId).getApplication());
          } else if (entityId instanceof Id.Artifact) {
            String version = null;
            try {
              version = getMetaClient().getVersion().getVersion();
            } catch (Exception e) {
              Assert.fail("Unable to retrieve CDAP version. Exception: " + e.getMessage());
            }
            return Id.Artifact.from(Id.Namespace.DEFAULT, PurchaseApp.class.getSimpleName(), version).equals(entityId);
          }
          return false;
        }
      };
    }
    return purchaseAppPredicate;
  }

  private Set<MetadataSearchResultRecord> searchMetadata(Id.Namespace namespace, String query,
                                                         MetadataSearchTargetType targetType) throws Exception {
    Set<MetadataSearchResultRecord> results = metadataClient.searchMetadata(namespace, query, targetType);
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }
}
