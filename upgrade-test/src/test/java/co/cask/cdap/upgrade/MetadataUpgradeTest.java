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
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryBuilder;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
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

  private static final ApplicationId PURCHASE_APP = TEST_NAMESPACE.app(PurchaseApp.APP_NAME);
  private static final ProgramId PURCHASE_HISTORY_BUILDER = PURCHASE_APP.mr(
    PurchaseHistoryBuilder.class.getSimpleName());
  private static final StreamId PURCHASE_STREAM = TEST_NAMESPACE.stream("purchaseStream");
  private static final StreamViewId PURCHASE_VIEW = PURCHASE_STREAM.view(
    PURCHASE_STREAM.getEntityName() + "View");
  private static final DatasetId HISTORY = TEST_NAMESPACE.dataset("history");
  private static final DatasetId FREQUENT_CUSTOMERS = TEST_NAMESPACE.dataset("frequentCustomers");
  private static final DatasetId USER_PROFILES = TEST_NAMESPACE.dataset("userProfiles");
  private static final DatasetId PURCHASES = TEST_NAMESPACE.dataset("purchases");
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
  private MetadataClient metadataClient;

  @Override
  protected void preStage() throws Exception {
    // deploy an application
    deployApplication(PurchaseApp.class);

    // create a view
    Schema viewSchema = Schema.recordOf("record", Schema.Field.of(PURCHASE_VIEW_FIELD,
                                                                  Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StreamViewClient viewClient = new StreamViewClient(getClientConfig(), getRestClient());
    viewClient.createOrUpdate(PURCHASE_VIEW,
                              new ViewSpecification(new FormatSpecification(Formats.AVRO, viewSchema)));

    // Add some user metadata
    MetadataClient metadataClient = getMetadataClient();
    metadataClient.addProperties(PURCHASE_APP.toId(), APP_PROPERTIES);
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP.toId(), MetadataScope.USER));

    metadataClient.addTags(PURCHASE_STREAM.toId(), STREAM_TAGS);
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM.toId(), 
                                                                             MetadataScope.USER));

    metadataClient.addTags(PURCHASE_HISTORY_BUILDER.toId(), MR_TAGS);
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER.toId(), 
                                                                         MetadataScope.USER));

    metadataClient.addTags(HISTORY.toId(), DS_TAGS);
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(HISTORY.toId(), MetadataScope.USER));

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
    MetadataClient metadataClient = getMetadataClient();
    // verify user metadata added prior to upgrade
    Assert.assertEquals(EXPECTED_APP_METADATA, metadataClient.getMetadata(PURCHASE_APP.toId(), MetadataScope.USER));
    Assert.assertEquals(EXPECTED_STREAM_METADATA, metadataClient.getMetadata(PURCHASE_STREAM.toId(), 
                                                                             MetadataScope.USER));
    Assert.assertEquals(EXPECTED_MR_METADATA, metadataClient.getMetadata(PURCHASE_HISTORY_BUILDER.toId(),
                                                                         MetadataScope.USER));
    Assert.assertEquals(EXPECTED_DS_METADATA, metadataClient.getMetadata(HISTORY.toId(),
                                                                         MetadataScope.USER));
    // verify search using user metadata added prior to upgrade
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_APP)),
      searchMetadata(TEST_NAMESPACE, "env:prod", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_STREAM)
      ),
      searchMetadata(TEST_NAMESPACE, "input", null)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER)
      ),
      searchMetadata(TEST_NAMESPACE, "process*", EntityTypeSimpleName.PROGRAM)
    );
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(HISTORY)
      ),
      searchMetadata(TEST_NAMESPACE, "output", EntityTypeSimpleName.ALL)
    );

    // there should be system metadata records for these entities
    verifySystemMetadata(PURCHASE_APP, true, true);
    verifySystemMetadata(HISTORY, true, true);
    // currently we don't have any properties for programs
    verifySystemMetadata(PURCHASE_HISTORY_BUILDER, false, true);
    verifySystemMetadata(PURCHASE_STREAM, true, true);

    // makes some searches: this should get system entities such as dataset, artifacts, flow, services, programs
    Set<MetadataSearchResultRecord> searchResults = filterNonPurchaseEntities(
      searchMetadata(TEST_NAMESPACE, "explore", EntityTypeSimpleName.ALL));
    // 5 = dataset: frequentCustomers + dataset: userProfiles + dataset: purchases + dataset: history +
    // stream: purchaseStream
    Assert.assertEquals(5, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(TEST_NAMESPACE, "batch",
                                                             EntityTypeSimpleName.ALL));
    // 6 = dataset: frequentCustomers + dataset: userProfiles +
    // dataset: purchases + dataset: history +  workflow: PurchaseHistoryWorkflow + mapreduce: PurchaseHistoryBuilder
    Assert.assertEquals(6, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(TEST_NAMESPACE, "realtime",
                                                             EntityTypeSimpleName.ALL));
    // 4 = service: CatalogLookup + service: UserProfileService + service: PurchaseHistoryService + flow: PurchaseFlow
    Assert.assertEquals(4, searchResults.size());

    // system metadata for app check
    searchResults = searchMetadata(TEST_NAMESPACE, PURCHASE_APP.getEntityName(),
                                   EntityTypeSimpleName.ALL);
    Assert.assertEquals(1, searchResults.size());

    // system metadata for stream check
    searchResults = searchMetadata(TEST_NAMESPACE, PURCHASE_STREAM.getEntityName(),
                                   EntityTypeSimpleName.ALL);
    // 3 = stream: purchaseStream + app: PurchaseHistory + view: purchaseStreamView
    Assert.assertEquals(3, searchResults.size());

    // perform schema searches
    searchResults = filterNonPurchaseEntities(searchMetadata(TEST_NAMESPACE, "price",
                                                             EntityTypeSimpleName.ALL));
    // 2 = dataset: purchases + dataset: history
    Assert.assertEquals(2, searchResults.size());

    searchResults = filterNonPurchaseEntities(searchMetadata(TEST_NAMESPACE, "lastname:string",
                                                             EntityTypeSimpleName.ALL));
    // 1 =  dataset: history
    Assert.assertEquals(1, searchResults.size());

    // search for view schema
    searchResults = searchMetadata(TEST_NAMESPACE, PURCHASE_VIEW_FIELD,
                                   EntityTypeSimpleName.ALL);
    Assert.assertEquals(1, searchResults.size());
  }

  private void verifySystemMetadata(NamespacedEntityId id, boolean checkProperties,
                                    boolean checkTags) throws Exception {
    Set<MetadataRecord> metadataRecords = getMetadataClient().getMetadata(id.toId(), MetadataScope.SYSTEM);
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
          NamespacedEntityId entityId = input.getEntityId();
          if (entityId instanceof DatasetId) {
            return ImmutableSet.of(HISTORY, USER_PROFILES, PURCHASES, FREQUENT_CUSTOMERS).contains(entityId);
          } else if (entityId instanceof StreamViewId) {
            return PURCHASE_VIEW.equals(entityId);
          } else if (entityId instanceof StreamId) {
            return PURCHASE_STREAM.equals(entityId);
          } else if (entityId instanceof ApplicationId) {
            return PURCHASE_APP.equals(entityId);
          } else if (entityId instanceof ProgramId) {
            return PURCHASE_APP.equals(((ProgramId) entityId).getParent());
          } else if (entityId instanceof ArtifactId) {
            String version = null;
            try {
              version = getMetaClient().getVersion().getVersion();
            } catch (Exception e) {
              Assert.fail("Unable to retrieve CDAP version. Exception: " + e.getMessage());
            }
            return TEST_NAMESPACE.artifact(PurchaseApp.class.getSimpleName(), version).equals(entityId);
          }
          return false;
        }
      };
    }
    return purchaseAppPredicate;
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespace, String query,
                                                         EntityTypeSimpleName targetType) throws Exception {
    Set<MetadataSearchResultRecord> results =
      getMetadataClient().searchMetadata(namespace.toId(), query, targetType).getResults();
    Set<MetadataSearchResultRecord> transformed = new HashSet<>();
    for (MetadataSearchResultRecord result : results) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return transformed;
  }

  private MetadataClient getMetadataClient() {
    if (metadataClient == null) {
      metadataClient = new MetadataClient(getClientConfig(), getRestClient());
    }
    return metadataClient;
  }
}
