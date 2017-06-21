package co.cask.cdap.security;

import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Test;

/**
 *
 */
public class NamespaceImpersonationAuthorizationTest extends AuthorizationTestBase {
  @Test
  public void testGrantAccess() throws Exception {
    testGrantAccess(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDeployApp() throws Exception {
    testDeployApp(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDeployAppUnauthorized() throws Exception {
    testDeployAppUnauthorized(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testCreatedDeletedPrivileges() throws Exception {
    testCreatedDeletedPrivileges(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testWriteWithReadAuth() throws Exception {
    testWriteWithReadAuth(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }

  @Test
  public void testDatasetInProgram() throws Exception {
    testDatasetInProgram(getNamespaceMeta(new NamespaceId("auth1"), null, null, null, null, null, null),
                         getNamespaceMeta(new NamespaceId("auth2"), null, null, null, null, null, null));
  }

  @Test
  public void testListEntities() throws Exception {
    testListEntities(getNamespaceMeta(TEST_NAMESPACE, null, null, null, null, null, null));
  }
}
