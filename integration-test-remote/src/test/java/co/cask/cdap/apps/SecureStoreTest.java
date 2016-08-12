package co.cask.cdap.apps;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.SecureStoreClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.test.AudiTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Christopher on 8/5/16.
 */
public class SecureStoreTest extends AudiTestBase {

  @Test
  public void testSecureStore() throws Exception {
    SecureStoreClient storeClient = new SecureStoreClient(getClientConfig(), getRestClient());

    //add keys
    SecureKeyId id = new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "key5");
    SecureKeyCreateRequest request = new SecureKeyCreateRequest("", "a", ImmutableMap.<String, String>of());
    storeClient.createKey(id, request);


    Assert.assertNotNull(storeClient.getData(id));
    storeClient.deleteKey(id);


    SecureKeyId id2 = new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "key2");
    SecureKeyCreateRequest request2 = new SecureKeyCreateRequest("", "b", ImmutableMap.<String, String>of());
    storeClient.createKey(id2, request2);
    SecureKeyId id3 = new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "key3");
    SecureKeyCreateRequest request3 = new SecureKeyCreateRequest("", "c", ImmutableMap.<String, String>of());
    storeClient.createKey(id3, request3);

    Assert.assertNotNull(storeClient.getData(id2));
    storeClient.deleteKey(id2);

    boolean getNonExistentKeyFailure = false;
    try {
      storeClient.getData(id2);
    } catch (Exception e) {
      getNonExistentKeyFailure = true;
    }
    Assert.assertTrue(getNonExistentKeyFailure);
    Assert.assertNotNull(storeClient.getData(id3));

    boolean duplicateKeyError = false;
    try {
      storeClient.createKey(id3, request3);
    } catch (Exception e) {
      duplicateKeyError = true;
    }
    Assert.assertTrue(duplicateKeyError);

  }
}
