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

package io.cdap.cdap.app.etl.google;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for tests, that contain all general methods.
 */
public class GoogleBaseTest extends UserCredentialsTestBase {
  protected static final String FILE_SOURCE_STAGE_NAME = "FileSource";
  protected static final String FILE_SINK_STAGE_NAME = "FileSink";

  protected void checkPluginExists(String pluginName, String pluginType, String artifact) {
    Preconditions.checkNotNull(pluginName);
    Preconditions.checkNotNull(pluginType);
    Preconditions.checkNotNull(artifact);

    try {
      Tasks.waitFor(true, () -> {
        try {
          final ArtifactId artifactId = TEST_NAMESPACE.artifact(artifact, version);
          List<PluginSummary> plugins =
            artifactClient.getPluginSummaries(artifactId, pluginType, ArtifactScope.SYSTEM);
          return plugins.stream().anyMatch(pluginSummary -> pluginName.equals(pluginSummary.getName()));
        } catch (ArtifactNotFoundException e) {
          // happens if the relevant artifact(s) were not added yet
          return false;
        }
      }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  protected static void startWorkFlow(ApplicationManager appManager, ProgramRunStatus expectedStatus) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(expectedStatus, 5, TimeUnit.MINUTES);
  }

  protected void checkRowsNumber(DeploymentDetails deploymentDetails, int expectedCount) throws Exception {
    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());
    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out",
                expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in",
                expectedCount, 10);
  }

  protected Path createFileSystemFolder(String path) throws IOException {
    return Files.createTempDirectory(path);
  }

  protected static String createFolder(Drive service, String folderName) throws IOException {
    File fileMetadata = new File();
    fileMetadata.setName(folderName);
    fileMetadata.setMimeType("application/vnd.google-apps.folder");

    File createdFolder = service.files().create(fileMetadata).setFields("id").execute();
    return createdFolder.getId();
  }

  protected static String createFile(Drive service, byte[] content, String name, String mime, String subMime,
                                     String folderId) throws IOException {
    File fileToWrite = new File();
    fileToWrite.setName(name);
    fileToWrite.setParents(Collections.singletonList(folderId));
    fileToWrite.setMimeType(mime);
    ByteArrayContent fileContent = new ByteArrayContent(subMime, content);

    File file = service.files().create(fileToWrite, fileContent)
      .setFields("id, parents, mimeType")
      .execute();
    return file.getId();
  }

  protected static void removeFile(Drive service, String fileId) throws IOException {
    service.files().delete(fileId).execute();
  }

  protected static List<File> getFiles(Drive drive, String parentFolderId) {
    try {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = drive.files().list()
        .setQ(String.format("'%s' in parents", parentFolderId))
        .setFields("nextPageToken, files(id, name, size, mimeType)");
      while (nextToken != null) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
      }
      return files;
    } catch (IOException e) {
      throw new RuntimeException("Issue during retrieving summary for files.", e);
    }
  }

  protected static void createFileSystemTextFile(Path dirPath, String name, String content) throws IOException {
    Path createdFile = Files.createTempFile(dirPath, name, null);
    Files.write(createdFile, content.getBytes());
  }

  protected DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                                Map<String, String> sinkProperties,
                                                String sourceStageName, String sinkStageName,
                                                String sourcePluginName, String sinkPluginName,
                                                ArtifactSelectorConfig sourceArtifact,
                                                ArtifactSelectorConfig sinkArtifact,
                                                String applicationName) throws Exception {
    ETLStage source = new ETLStage(sourceStageName,
                                   new ETLPlugin(sourcePluginName,
                                                 BatchSource.PLUGIN_TYPE,
                                                 sourceProperties,
                                                 sourceArtifact));
    ETLStage sink = new ETLStage(sinkStageName, new ETLPlugin(sinkPluginName,
                                                              BatchSink.PLUGIN_TYPE,
                                                              sinkProperties,
                                                              sinkArtifact));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  protected static class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    public DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }
}
