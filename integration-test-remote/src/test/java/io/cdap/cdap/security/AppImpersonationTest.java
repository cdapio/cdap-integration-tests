/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.security;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.apps.appimpersonation.FileGeneratorApp;
import io.cdap.cdap.apps.appimpersonation.FileProcessorApp;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.explore.service.ExploreException;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tests basic impersonation for different users across the same namespace.
 */
public class AppImpersonationTest extends AudiTestBase {
  private static final NamespaceId NAMESPACE_ID = new NamespaceId("imp_ns");
  private static final String ALICE = "alice";
  private static final String BOB = "bob";
  private static final String EVE = "eve";
  private static final String GROUP = "deployers";
  private static final String VERSION = "1.0.0";

  @Test
  public void test() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();
    try {
      namespaceClient.get(NAMESPACE_ID);
      Assert.fail();
    } catch (NamespaceNotFoundException expected) {
      // expected
    }

    registerForDeletion(NAMESPACE_ID);
    NamespaceMeta.Builder nsMetaBuilder = new NamespaceMeta.Builder()
      .setName(NAMESPACE_ID)
      .setPrincipal(ALICE)
      .setGroupName(GROUP)
      .setKeytabURI(SecurityTestUtils.getKeytabURIforPrincipal(ALICE, getMetaClient().getCDAPConfig()));
    namespaceClient.create(nsMetaBuilder.build());
    // test if namespace exists
    namespaceClient.get(NAMESPACE_ID);

    ArtifactId generator = NAMESPACE_ID.artifact(FileGeneratorApp.class.getSimpleName(), VERSION);
    ArtifactId processor = NAMESPACE_ID.artifact(FileProcessorApp.class.getSimpleName(), VERSION);
    getTestManager().addAppArtifact(generator, FileGeneratorApp.class);
    getTestManager().addAppArtifact(processor, FileProcessorApp.class);

    ArtifactSummary generatorSummary = new ArtifactSummary(FileGeneratorApp.class.getSimpleName(), VERSION);
    ArtifactSummary processorSummary = new ArtifactSummary(FileProcessorApp.class.getSimpleName(), VERSION);

    ApplicationId generatorId = NAMESPACE_ID.app(FileGeneratorApp.class.getSimpleName());
    ApplicationId processorId = NAMESPACE_ID.app(FileProcessorApp.class.getSimpleName());

    FileGeneratorApp.FileGeneratorAppConfig config = new FileGeneratorApp.FileGeneratorAppConfig();
    config.resultPerms = "770"; // Group should be given full permissions
    config.resultGroup = GROUP; // Results should be owned by group GROUP
    config.resultGrants = String.format("{'@%s':'RW'}", GROUP); // Group should be given HBase RW permissions

    ApplicationManager generatorAppManager
      = deployApplication(generatorId, new AppRequest<>(generatorSummary, config, BOB));
    ApplicationManager processorAppManager
      = deployApplication(processorId, new AppRequest<>(processorSummary, null, EVE));

    WorkerManager generatorWorkerManager
      = generatorAppManager.getWorkerManager(FileGeneratorApp.FileGeneratorWorker.class.getSimpleName());
    WorkflowManager processorWorkflowManager
      = processorAppManager.getWorkflowManager(FileProcessorApp.FileProcessWorkflow.class.getSimpleName());

    // Ensure that there are no results before starting
    ExploreExecutionResult results = new QueryClient(getClientConfig())
      .execute(NAMESPACE_ID,
               String.format("SELECT * FROM cdap_%s.dataset_consumingstate LIMIT 5", NAMESPACE_ID.getNamespace()))
      .get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertFalse(results.hasNext());

    generatorWorkerManager.start();
    generatorWorkerManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    TimeUnit.SECONDS.sleep(15);

    generatorWorkerManager.stop();
    generatorWorkerManager.waitForRun(ProgramRunStatus.KILLED, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    processorWorkflowManager.start();
    processorWorkflowManager
      .waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    processorWorkflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Ensure that there is at least one result
    results = new QueryClient(getClientConfig())
      .execute(NAMESPACE_ID,
               String.format("SELECT * FROM cdap_%s.dataset_consumingstate LIMIT 5", NAMESPACE_ID.getNamespace()))
      .get();
    Assert.assertEquals(QueryStatus.OpStatus.FINISHED, results.getStatus().getStatus());
    Assert.assertTrue(results.hasNext());

    // Update the namespaceClient to disable explore as principal
    namespaceClient.updateProperties(NAMESPACE_ID, nsMetaBuilder.setExploreAsPrincipal(false).build());

    try {
      new QueryClient(getClientConfig())
        .execute(NAMESPACE_ID,
                 String.format("SELECT * FROM cdap_%s.dataset_consumingstate LIMIT 5", NAMESPACE_ID.getNamespace()))
        .get();
      Assert.fail("Expected an ExecutionException to be thrown");
    } catch (ExecutionException e) {
      // expected
      Assert.assertTrue(e.getCause() instanceof ExploreException);
    }
  }
}
