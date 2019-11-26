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

package io.cdap.cdap.app.etl.google.drive;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests reading to and writing from Google Drive within a sandbox cluster.
 */
public class GoogleDriveTest extends UserCredentialsTestBase {

  @Rule
  public TestName testName = new TestName();

  protected static final ArtifactSelectorConfig GOOGLE_DRIVE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-drive-plugins", "[0.0.0, 100.0.0)");
  protected static final ArtifactSelectorConfig FILE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "core-plugins", "[0.0.0, 100.0.0)");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String GOOGLE_DRIVE_PLUGIN_NAME = "GoogleDrive";
  private static final String FILE_PLUGIN_NAME = "File";
  private static final int GENERATED_NAME_LENGTH = 16;
  private static final String TEXT_PLAIN_MIME = "text/plain";
  private static final String TEXT_CSV_MIME = "text/csv";
  private static final String UNDEFINED_MIME = "application/octet-stream";

  private static final String TEST_TEXT_FILE_NAME = "textFile";
  private static final String TEST_DOC_FILE_NAME = "docFile";
  private static final String TEST_SHEET_FILE_NAME = "sheetFile";
  private static final String TEST_TEXT_FILE_CONTENT = "text file content";
  private static final String TEST_DOC_FILE_CONTENT = "Google Document file content";
  private static final String TEST_SHEET_FILE_CONTENT = "a,b,c\r\n,d,e";
  private static final String DRIVE_SOURCE_STAGE_NAME = "GoogleDriveSource";
  private static final String DRIVE_SINK_STAGE_NAME = "GoogleDriveSink";
  private static final String FILE_SOURCE_STAGE_NAME = "FileSource";
  private static final String FILE_SINK_STAGE_NAME = "FileSink";
  public static final String TMP_FOLDER_NAME = "googleDriveTestFolder";

  private static Drive service;
  private String sourceFolderId;
  private String sinkFolderId;
  private String testTextFileId;
  private String testDocFileId;
  private String testSheetFileId;
  private Path tmpFolder;

  @BeforeClass
  public static void setupDrive() throws GeneralSecurityException, IOException {
    final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

    GoogleCredential credential = new GoogleCredential.Builder()
      .setTransport(HTTP_TRANSPORT)
      .setJsonFactory(JSON_FACTORY)
      .setClientSecrets(getClientId(),
                        getClientSecret())
      .build();
    credential.setRefreshToken(getRefreshToken());

    service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
  }

  @Before
  public void testClassSetup() throws IOException {
    ImmutableList.of(ImmutableList.of(GOOGLE_DRIVE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, "cdap-data-pipeline"),
                     ImmutableList.of(GOOGLE_DRIVE_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, "cdap-data-pipeline"))
      .forEach((pluginInfo) -> checkPluginExists(pluginInfo.get(0), pluginInfo.get(1), pluginInfo.get(2)));

    String sourceFolderName = RandomStringUtils.randomAlphanumeric(16);
    String sinkFolderName = RandomStringUtils.randomAlphanumeric(16);

    sourceFolderId = createFolder(service, sourceFolderName);
    sinkFolderId = createFolder(service, sinkFolderName);

    testTextFileId = createFile(service, TEST_TEXT_FILE_CONTENT.getBytes(), TEST_TEXT_FILE_NAME,
                                TEXT_PLAIN_MIME, null, sourceFolderId);
    testDocFileId = createFile(service, TEST_DOC_FILE_CONTENT.getBytes(), TEST_DOC_FILE_NAME,
                               "application/vnd.google-apps.document", TEXT_PLAIN_MIME, sourceFolderId);
    testSheetFileId = createFile(service, TEST_SHEET_FILE_CONTENT.getBytes(), TEST_SHEET_FILE_NAME,
                                 "application/vnd.google-apps.spreadsheet", TEXT_CSV_MIME, sourceFolderId);
    tmpFolder = createFileSystemFolder(TMP_FOLDER_NAME);
  }

  @After
  public void removeFolders() throws IOException {
    removeFile(service, testTextFileId);
    removeFile(service, testDocFileId);
    removeFile(service, testSheetFileId);
    removeFile(service, sourceFolderId);
    removeFile(service, sinkFolderId);

    Files.walk(tmpFolder)
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .forEach(java.io.File::delete);
  }

  @Test
  public void testBinaryOnly() throws Exception {
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary");
      }
    };
    Map<String, String> sinkProps = getDriveSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(1, destFiles.size());

    File textFile = destFiles.get(0);

    Assert.assertEquals(UNDEFINED_MIME, textFile.getMimeType());
    Assert.assertNotEquals(TEST_TEXT_FILE_NAME, textFile.getName());
    Assert.assertEquals(GENERATED_NAME_LENGTH, textFile.getName().length());

    String content = getFileContent(textFile.getId());
    Assert.assertEquals(TEST_TEXT_FILE_CONTENT, content);
  }

  @Test
  public void testDocFileOnly() throws Exception {
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "documents");
      }
    };
    Map<String, String> sinkProps = getDriveSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(1, destFiles.size());

    File docFile = destFiles.get(0);

    Assert.assertEquals(UNDEFINED_MIME, docFile.getMimeType());
    Assert.assertNotEquals(TEST_TEXT_FILE_NAME, docFile.getName());
    Assert.assertEquals(GENERATED_NAME_LENGTH, docFile.getName().length());

    String content = getFileContent(docFile.getId());
    // check BOM
    Assert.assertEquals('\uFEFF', content.charAt(0));
    Assert.assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
  }

  @Test
  public void testAllFileTypes() throws Exception {
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary,documents,spreadsheets");
      }
    };
    Map<String, String> sinkProps = getDriveSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      Assert.assertEquals(UNDEFINED_MIME, file.getMimeType());
      Assert.assertNotEquals(TEST_TEXT_FILE_NAME, file.getName());
      Assert.assertNotEquals(TEST_DOC_FILE_NAME, file.getName());
      Assert.assertNotEquals(TEST_SHEET_FILE_NAME, file.getName());
      Assert.assertEquals(GENERATED_NAME_LENGTH, file.getName().length());
    });
  }

  @Test
  public void testAllFileTypesNamed() throws Exception {
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary,documents,spreadsheets");
        put("fileMetadataProperties", "name");
      }
    };
    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getDriveSinkMinimalDefaultConfigs());
        put("schemaNameFieldName", "name");
      }
    };

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      Assert.assertEquals(UNDEFINED_MIME, file.getMimeType());
      Assert.assertNotNull(file.getName());
      try {
        String fileName = file.getName();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            Assert.assertEquals(TEST_TEXT_FILE_CONTENT, content);
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            Assert.assertEquals('\uFEFF', content.charAt(0));
            Assert.assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            break;
          case TEST_SHEET_FILE_NAME:
            Assert.assertEquals(TEST_SHEET_FILE_CONTENT, content);
            break;
          default:
            Assert.fail(String.format("Invalid file name after pipeline completion: '%s', content: '%s'",
                                      fileName, content));
        }
      } catch (IOException e) {
        Assert.fail(String.format("Exception during test results check: '%s'", e.getMessage()));
      }
    });
  }

  @Test
  public void testAllFileTypesNamedAndMimed() throws Exception {
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary,documents,spreadsheets");
        put("fileMetadataProperties", "name,mimeType");
      }
    };
    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getDriveSinkMinimalDefaultConfigs());
        put("schemaNameFieldName", "name");
        put("schemaMimeFieldName", "mimeType");
      }
    };

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      Assert.assertNotNull(file.getName());
      String fileName = null;
      String mimeType = null;
      try {
        fileName = file.getName();
        mimeType = file.getMimeType();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            Assert.assertEquals(TEST_TEXT_FILE_CONTENT, content);
            Assert.assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            Assert.assertEquals('\uFEFF', content.charAt(0));
            Assert.assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            Assert.assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_SHEET_FILE_NAME:
            Assert.assertEquals(TEST_SHEET_FILE_CONTENT, content);
            Assert.assertEquals(TEXT_CSV_MIME, mimeType);
            break;
          default:
            Assert.fail(
              String.format("Invalid file name after pipeline completion: '%s', content: '%s', mime type: '%s'",
                            fileName, content, mimeType));
        }
      } catch (IOException e) {
        Assert.fail(String.format("Exception during test results check: '%s', file name '%s', mimeType '%s'",
                                  e.getMessage(),
                                  fileName == null ? "unknown" : fileName,
                                  mimeType == null ? "unknown" : mimeType));
      }
    });
  }

  @Test
  public void testPartitionSize() throws Exception {
    int testMaxPartitionSize = 10;

    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary,documents,spreadsheets");
        put("fileMetadataProperties", "name,mimeType");
        put("maxPartitionSize", Integer.toString(testMaxPartitionSize));
      }
    };
    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getDriveSinkMinimalDefaultConfigs());
        put("schemaNameFieldName", "name");
        put("schemaMimeFieldName", "mimeType");
      }
    };

    DeploymentDetails deploymentDetails =
      deployGoogleDriveApplication(sourceProps, sinkProps,
                                   GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 4);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(4, destFiles.size());

    // flags to check partitioning work
    boolean firstTextPart = false;
    boolean secondTextPart = false;
    List<String> parts = new ArrayList<>();

    // Document and Sheets don't support partitioning
    for (File file : destFiles) {
      Assert.assertNotNull(file.getName());
      try {
        String fileName = file.getName();
        String mimeType = file.getMimeType();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            Assert.assertNotEquals(TEST_TEXT_FILE_CONTENT, content);
            Assert.assertEquals(TEXT_PLAIN_MIME, mimeType);
            parts.add(content);
            if (content.equals(TEST_TEXT_FILE_CONTENT.substring(0, testMaxPartitionSize))) {
              firstTextPart = true;
            }
            if (content.equals(TEST_TEXT_FILE_CONTENT.substring(testMaxPartitionSize))) {
              secondTextPart = true;
            }
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            Assert.assertEquals('\uFEFF', content.charAt(0));
            Assert.assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            Assert.assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_SHEET_FILE_NAME:
            Assert.assertEquals(TEST_SHEET_FILE_CONTENT, content);
            Assert.assertEquals(TEXT_CSV_MIME, mimeType);
            break;
          default:
            Assert.fail(
              String.format("Invalid file name after pipeline completion: '%s', content: '%s', mime type: '%s'",
                            fileName, content, mimeType));
        }
      } catch (IOException e) {
        Assert.fail(String.format("Exception during test results check: '%s'", e.getMessage()));
      }
    }
    Assert.assertTrue(String.format("Text file was separated incorrectly: '%s'", parts.toString()), firstTextPart);
    Assert.assertTrue(String.format("Text file was separated incorrectly: '%s'", parts.toString()), secondTextPart);
  }

  @Test
  public void testWithFileSource() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME, TEST_TEXT_FILE_CONTENT);

    Map<String, String> sourceProps = getFileSourceMinimalDefaultConfigs();
    Map<String, String> sinkProps = getDriveSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, FILE_SOURCE_STAGE_NAME, DRIVE_SINK_STAGE_NAME,
                        FILE_PLUGIN_NAME, GOOGLE_DRIVE_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> destFiles = getFiles(sinkFolderId);
    Assert.assertEquals(1, destFiles.size());

    File textFile = destFiles.get(0);

    Assert.assertEquals(UNDEFINED_MIME, textFile.getMimeType());
    Assert.assertNotEquals(TEST_TEXT_FILE_NAME, textFile.getName());
    Assert.assertEquals(GENERATED_NAME_LENGTH, textFile.getName().length());

    String content = getFileContent(textFile.getId());
    Assert.assertEquals(TEST_TEXT_FILE_CONTENT, content);
  }

  @Test
  public void testWithFileSink() throws Exception {
    int testMaxPartitionSize = 50;
    String testFileName = "Image.png";
    String testFileMime = "image/png";
    byte[] testPNGContent = new byte[]{-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 5, 0,
      0, 0, 5, 8, 2, 0, 0, 0, 2, 13, -79, -78, 0, 0, 0, 9, 112, 72, 89, 115, 0, 0, 11, 19, 0, 0, 11, 19, 1, 0, -102,
      -100, 24, 0, 0, 0, 7, 116, 73, 77, 69, 7, -29, 10, 18, 13, 43, 15, 2, -77, 55, -110, 0, 0, 0, 25, 116, 69, 88,
      116, 67, 111, 109, 109, 101, 110, 116, 0, 67, 114, 101, 97, 116, 101, 100, 32, 119, 105, 116, 104, 32, 71, 73,
      77, 80, 87, -127, 14, 23, 0, 0, 0, 40, 73, 68, 65, 84, 8, -41, 93, -117, 65, 10, 0, 48, 12, -62, -30, -1, 31,
      -99, 29, 108, -95, -52, -125, 72, -44, -88, 73, 84, 0, 8, -85, 65, 106, 83, -3, 44, -5, -6, -6, 7, -62, -105, 32,
      -23, 115, 33, -2, -49, 0, 0, 0, 0, 73, 69, 78, 68, -82, 66, 96, -126};
    int contentLength = testPNGContent.length;

    // create png file with metadata in Google Drive
    // size: 174 bytes
    createFile(service, testPNGContent, testFileName, "image/png", null, sourceFolderId);

    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getDriveSourceMinimalDefaultConfigs());
        put("fileTypesToPull", "binary,documents,spreadsheets");
        put("bodyFormat", "bytes");
        put("filter", String.format("name='%s'", testFileName));
        put("fileMetadataProperties", "name,mimeType,size,imageMediaMetadata.width," +
          "imageMediaMetadata.height,imageMediaMetadata.rotation");
        put("maxPartitionSize", Integer.toString(testMaxPartitionSize));
      }
    };
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, DRIVE_SOURCE_STAGE_NAME, FILE_SINK_STAGE_NAME,
                        GOOGLE_DRIVE_PLUGIN_NAME, FILE_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 4);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(4, destFiles.size());

    JsonParser jsonParser = new JsonParser();

    Map<Integer, byte[]> partitionedContent = new HashMap<>();
    for (Path destFile : destFiles) {
      List<String> fileLines = null;
      try {
        fileLines = Files.readAllLines(destFile);
      } catch (IOException e) {
        Assert.fail(String.format("Exception during reading file '%s': '%s'", destFile.toString(), e.getMessage()));
      }
      String fileContent = String.join(",", fileLines);
      JsonElement rootElement = jsonParser.parse(fileContent);
      Assert.assertTrue(rootElement.isJsonObject());

      JsonObject rootObject = rootElement.getAsJsonObject();

      // Entries: name, mimeType, size, imageMediaMetadata, offset, body
      Assert.assertEquals(6, rootObject.entrySet().size());
      Assert.assertEquals(testFileName, rootObject.get("name").getAsString());
      Assert.assertEquals(testFileMime, rootObject.get("mimeType").getAsString());
      Assert.assertEquals(contentLength, rootObject.get("size").getAsInt());

      JsonObject imageMediaMetadataObject = rootObject.get("imageMediaMetadata").getAsJsonObject();

      // Image metadata entries: width, height, rotation
      Assert.assertEquals(3, imageMediaMetadataObject.entrySet().size());
      Assert.assertEquals(5, imageMediaMetadataObject.get("width").getAsInt());
      Assert.assertEquals(5, imageMediaMetadataObject.get("height").getAsInt());
      Assert.assertEquals(0, imageMediaMetadataObject.get("rotation").getAsInt());

      // collect bodies for next check
      int resultOffset = rootObject.get("offset").getAsInt();
      JsonArray bytes = rootObject.get("body").getAsJsonArray();
      byte[] resultBody = new byte[bytes.size()];
      for (int i = 0; i < bytes.size(); i++) {
        resultBody[i] = bytes.get(i).getAsByte();
      }
      partitionedContent.put(resultOffset, resultBody);
    }
    byte[] assembledContent = new byte[contentLength];
    ByteBuffer buffer = ByteBuffer.wrap(assembledContent);
    for (Map.Entry<Integer, byte[]> part : partitionedContent.entrySet()) {
      buffer.position(part.getKey());
      buffer.put(part.getValue());
    }
    Assert.assertArrayEquals(testPNGContent, buffer.array());
  }

  private Map<String, String> getDriveSourceMinimalDefaultConfigs() {
    return new HashMap<String, String>() {
      {
        put("referenceName", "ref");
        put("directoryIdentifier", sourceFolderId);
        put("modificationDateRange", "lifetime");
        put("fileTypesToPull", "binary");
        put("maxPartitionSize", "0");
        put("bodyFormat", "bytes");
        put("docsExportingFormat", "text/plain");
        put("sheetsExportingFormat", "text/csv");
        put("drawingsExportingFormat", "image/svg+xml");
        put("presentationsExportingFormat", "text/plain");
        put("authType", "oAuth2");
        put("clientId", getClientId());
        put("clientSecret", getClientSecret());
        put("refreshToken", getRefreshToken());
        put("maxRetryCount", "8");
        put("maxRetryWait", "200");
        put("maxRetryJitterWait", "100");
      }
    };
  }

  private Map<String, String> getFileSourceMinimalDefaultConfigs() {
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    return new HashMap<String, String>() {
      {
        put("path", tmpFolder.toString());
        put("referenceName", "fileref");
        put("format", "blob");
        put("schema", fileSchema.toString());
      }
    };
  }

  private Map<String, String> getDriveSinkMinimalDefaultConfigs() {
    return new HashMap<String, String>() {
      {
        put("referenceName", "refd");
        put("directoryIdentifier", sinkFolderId);
        put("schemaBodyFieldName", "body");
        put("authType", "oAuth2");
        put("clientId", getClientId());
        put("clientSecret", getClientSecret());
        put("refreshToken", getRefreshToken());
        put("maxRetryCount", "8");
        put("maxRetryWait", "200");
        put("maxRetryJitterWait", "100");
      }
    };
  }

  private Map<String, String> getFileSinkMinimalDefaultConfigs() {
    return new HashMap<String, String>() {
      {
        put("suffix", "yyyy-MM-dd-HH-mm");
        put("path", tmpFolder.toString());
        put("referenceName", "fileref");
        put("format", "json");
      }
    };
  }

  protected void startWorkFlow(ApplicationManager appManager, ProgramRunStatus expectedStatus) throws Exception {
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(expectedStatus, 5, TimeUnit.MINUTES);
  }

  private void checkRowsNumber(DeploymentDetails deploymentDetails, int expectedCount) throws Exception {
    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());
    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out",
                expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in",
                expectedCount, 10);
  }

  private void checkPluginExists(String pluginName, String pluginType, String artifact) {
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

  private DeploymentDetails deployGoogleDriveApplication(Map<String, String> sourceProperties,
                                                         Map<String, String> sinkProperties,
                                                         String applicationName) throws Exception {
    return deployApplication(sourceProperties, sinkProperties,
                             DRIVE_SOURCE_STAGE_NAME, DRIVE_SINK_STAGE_NAME,
                             GOOGLE_DRIVE_PLUGIN_NAME, GOOGLE_DRIVE_PLUGIN_NAME,
                             GOOGLE_DRIVE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT, applicationName);
  }

  private DeploymentDetails deployApplication(Map<String, String> sourceProperties, Map<String, String> sinkProperties,
                                              String sourceStageName, String sinkStageName,
                                              String sourcePluginName, String sinkPluginName,
                                              ArtifactSelectorConfig sourceArtifact,
                                              ArtifactSelectorConfig sinkArtifact, String applicationName)
    throws Exception {
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

  private static String createFile(Drive service, byte[] content, String name, String mime, String subMime,
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

  private static String createFolder(Drive service, String folderName) throws IOException {
    File fileMetadata = new File();
    fileMetadata.setName(folderName);
    fileMetadata.setMimeType("application/vnd.google-apps.folder");

    File createdFolder = service.files().create(fileMetadata).setFields("id").execute();
    return createdFolder.getId();
  }

  private static void removeFile(Drive service, String fileId) throws IOException {
    service.files().delete(fileId).execute();
  }

  private static List<File> getFiles(String parentFolderId) {
    try {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = service.files().list()
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

  private static String getFileContent(String fileId) throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    Drive.Files.Get get = service.files().get(fileId);

    get.executeMediaAndDownloadTo(outputStream);
    return ((ByteArrayOutputStream) outputStream).toString();
  }

  private static Path createFileSystemFolder(String path) throws IOException {
    return Files.createTempDirectory(path);
  }

  private static void createFileSystemTextFile(Path dirPath, String name, String content) throws IOException {
    Path createdFile = Files.createTempFile(dirPath, name, null);
    Files.write(createdFile, content.getBytes());
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
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
