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

package io.cdap.cdap.app.etl.google.sheets;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.NumberFormat;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.google.GoogleBaseTest;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.proto.ProgramRunStatus;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests reading to and writing from Google Sheets within a sandbox cluster.
 */
public class GoogleSheetsTest extends GoogleBaseTest {

  @Rule
  public TestName testName = new TestName();

  protected static final ArtifactSelectorConfig GOOGLE_DRIVE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-drive-plugins", "[0.0.0, 100.0.0)");
  protected static final ArtifactSelectorConfig FILE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "core-plugins", "[0.0.0, 100.0.0)");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String GOOGLE_SHEETS_PLUGIN_NAME = "GoogleSheets";
  private static final String FILE_PLUGIN_NAME = "File";
  private static final String TEST_SHEET_FILE_NAME = "sheetFile";
  private static final String TEXT_CSV_MIME = "text/csv";
  private static final String DEFAULT_SPREADSHEET_NAME = "sp0";
  private static final String DEFAULT_SHEET_NAME = "s0";
  private static final String TEST_TEXT_FILE_NAME = "textFile";
  private static final String SHEETS_SOURCE_STAGE_NAME = "GoogleSheetsSource";
  private static final String SHEETS_SINK_STAGE_NAME = "GoogleSheetsSink";

  public static final String TMP_FOLDER_NAME = "googleSheetsTestFolder";

  private static Drive driveService;
  private static Sheets sheetsService;
  private String sourceFolderId;
  private String sinkFolderId;
  private String testSourceFileId;
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

    driveService = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
    sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
  }

  @Before
  public void testClassSetup() throws IOException {
    ImmutableList.of(ImmutableList.of(GOOGLE_SHEETS_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, "cdap-data-pipeline"),
                     ImmutableList.of(GOOGLE_SHEETS_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, "cdap-data-pipeline"))
      .forEach((pluginInfo) -> checkPluginExists(pluginInfo.get(0), pluginInfo.get(1), pluginInfo.get(2)));

    String sourceFolderName = RandomStringUtils.randomAlphanumeric(16);
    String sinkFolderName = RandomStringUtils.randomAlphanumeric(16);

    sourceFolderId = createFolder(driveService, sourceFolderName);
    sinkFolderId = createFolder(driveService, sinkFolderName);

    testSourceFileId = createFile(driveService, "".getBytes(), TEST_SHEET_FILE_NAME,
                                  "application/vnd.google-apps.spreadsheet", TEXT_CSV_MIME, sourceFolderId);
    tmpFolder = createFileSystemFolder(TMP_FOLDER_NAME);
  }


  @After
  public void removeFolders() throws IOException {
    if (testSourceFileId != null) {
      removeFile(driveService, testSourceFileId);
    }
    if (sourceFolderId != null) {
      removeFile(driveService, sourceFolderId);
    }
    if (sinkFolderId != null) {
      removeFile(driveService, sinkFolderId);
    }

    Files.walk(tmpFolder)
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .forEach(java.io.File::delete);
  }

  @Test
  public void testSourceFileAllRecords() throws Exception {
    final int recordsToRead = 15;
    final int populatedRows = 10;
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
        put("lastDataRow", String.valueOf(recordsToRead));
        put("skipEmptyData", "false");
      }
    };
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId, generateSimpleRows(populatedRows, 5));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, FILE_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsToRead);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(recordsToRead, fileLines.size());
    Assert.assertEquals(populatedRows, getNonNullRowsCount(fileLines));
  }

  @Test
  public void testSourceFileNonEmptyRecords() throws Exception {
    final int recordsToRead = 15;
    final int populatedRows = 10;
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
        put("lastDataRow", String.valueOf(recordsToRead));
      }
    };
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId, generateSimpleRows(populatedRows, 5));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, FILE_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, populatedRows);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(populatedRows, fileLines.size());
    Assert.assertEquals(populatedRows, getNonNullRowsCount(fileLines));
  }

  @Test
  public void testSourceSinkSingleFile() throws Exception {
    final int recordsPerName = 2;
    final int columnsNumber = 5;
    final List<String> names = Arrays.asList("name1", "name2");
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
        put("lastDataRow", String.valueOf(recordsPerName * names.size()));
      }
    };
    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateRowsWithNames(recordsPerName, columnsNumber, names));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, SHEETS_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME,
                        GOOGLE_DRIVE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsPerName * names.size());

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, file.getName());

    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Assert.assertEquals(DEFAULT_SHEET_NAME, spreadsheet.getSheets().get(0).getProperties().getTitle());
    Assert.assertEquals(recordsPerName * names.size(),
                        spreadsheet.getSheets().get(0).getData().get(0).getRowData().size());
    for (RowData rowData : spreadsheet.getSheets().get(0).getData().get(0).getRowData()) {
      Assert.assertEquals(columnsNumber, rowData.getValues().size());
    }
  }

  @Test
  public void testSourceSinkSeparateFiles() throws Exception {
    final int recordsPerName = 2;
    final int columnsNumber = 5;
    final List<String> names = Arrays.asList("name1", "name2");
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
        put("lastDataRow", String.valueOf(recordsPerName * names.size()));
      }
    };
    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSinkMinimalDefaultConfigs());

        // set first column value as file name
        put("schemaSpreadsheetNameFieldName", "A");
      }
    };

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateRowsWithNames(recordsPerName, columnsNumber, names));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, SHEETS_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME,
                        GOOGLE_DRIVE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsPerName * names.size());

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(2, resultFiles.size());

    for (File file : resultFiles) {
      Assert.assertTrue(names.contains(file.getName()));

      String fileId = file.getId();
      Spreadsheet spreadsheet = getSpreadsheet(fileId);

      Assert.assertTrue(names.contains(spreadsheet.getProperties().getTitle()));
      Assert.assertNotNull(spreadsheet.getSheets());
      Assert.assertEquals(1, spreadsheet.getSheets().size());
      Assert.assertEquals(DEFAULT_SHEET_NAME, spreadsheet.getSheets().get(0).getProperties().getTitle());
      Assert.assertEquals(recordsPerName,
                          spreadsheet.getSheets().get(0).getData().get(0).getRowData().size());
      for (RowData rowData : spreadsheet.getSheets().get(0).getData().get(0).getRowData()) {
        Assert.assertEquals(columnsNumber - 1, rowData.getValues().size());
      }
    }
  }

  @Test
  public void testFileSinkMergeCells() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                             "{\"name\": \"test\",\"array\":[true,false]}");

    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    schemaFields.add(Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getFileSourceMinimalDefaultConfigs());
        put("schema", fileSchema.toString());
      }
    };

    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSinkMinimalDefaultConfigs());
        put("mergeDataCells", "true");
      }
    };

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, FILE_SOURCE_STAGE_NAME, SHEETS_SINK_STAGE_NAME,
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check merges
    Assert.assertNotNull(sheet.getMerges());
    Assert.assertEquals(1, sheet.getMerges().size());

    // check data cells
    List<RowData> rows = sheet.getData().get(0).getRowData();
    Assert.assertEquals(2, rows.size());
    if (rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(1).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(1).getUserEnteredValue().getBoolValue());

      Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(2)
                            .setStartColumnIndex(0).setEndColumnIndex(1).setSheetId(sheet.getProperties().getSheetId()),
                          sheet.getMerges().get(0));
    } else if (rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(0).getUserEnteredValue().getBoolValue());

      Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(2)
                            .setStartColumnIndex(1).setEndColumnIndex(2).setSheetId(sheet.getProperties().getSheetId()),
                          sheet.getMerges().get(0));
    } else {
      Assert.fail("Invalid value for first cell of the first row in the result spreadSheet.");
    }
  }

  @Test
  public void testFileSinkFlattenCells() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                             "{\"name\": \"test\",\"array\":[true,false]}");

    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    schemaFields.add(Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))));
    Schema fileSchema = Schema.recordOf("blob", schemaFields);
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getFileSourceMinimalDefaultConfigs());
        put("schema", fileSchema.toString());
      }
    };

    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, FILE_SOURCE_STAGE_NAME, SHEETS_SINK_STAGE_NAME,
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check cell values
    List<RowData> rows = sheet.getData().get(0).getRowData();
    Assert.assertEquals(2, rows.size());

    ExtendedValue firstValueInFirstRow = rows.get(0).getValues().get(0).getUserEnteredValue();
    if (firstValueInFirstRow.getStringValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(1).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(1).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(1).getUserEnteredValue().getBoolValue());
    } else if (rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(0).getUserEnteredValue().getBoolValue());
    } else {
      Assert.fail(String.format("Invalid value '%s' for first cell of the first row in the result spreadSheet.",
                                firstValueInFirstRow));
    }

    // check merges
    Assert.assertNull(sheet.getMerges());
  }

  @Test
  public void testFileSinkRecords() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                             "{\"record\":{\"field0\":true,\"field1\":\"test\"}}");

    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("record", Schema.recordOf("record", Arrays.asList(
      Schema.Field.of("field0", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
    ))));
    Schema fileSchema = Schema.recordOf("blob", schemaFields);
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getFileSourceMinimalDefaultConfigs());
        put("schema", fileSchema.toString());
      }
    };

    Map<String, String> sinkProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSinkMinimalDefaultConfigs());
        put("writeSchema", "true");
      }
    };

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, FILE_SOURCE_STAGE_NAME, SHEETS_SINK_STAGE_NAME,
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check cell values
    List<RowData> rows = sheet.getData().get(0).getRowData();
    // two rows for header and single for data
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("record", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
    Assert.assertNotNull(sheet.getMerges());
    Assert.assertEquals(1, sheet.getMerges().size());
    Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(1)
                          .setStartColumnIndex(0).setEndColumnIndex(2).setSheetId(sheet.getProperties().getSheetId()),
                        sheet.getMerges().get(0));

    ExtendedValue firstSubHeaderValue = rows.get(1).getValues().get(0).getUserEnteredValue();
    if ("field0".equals(firstSubHeaderValue.getStringValue())) {
      Assert.assertEquals("field1", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(2).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(2).getValues().get(0).getUserEnteredValue().getBoolValue());
    } else if ("field1".equals(firstSubHeaderValue.getStringValue())) {
      Assert.assertEquals("field0", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(2).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(2).getValues().get(1).getUserEnteredValue().getBoolValue());
    } else {
      Assert.fail(String.format("Invalid value '%s' for first sub-column name in the result spreadSheet.",
                                firstSubHeaderValue));
    }
  }

  @Test
  public void testMetadata() throws Exception {
    final String metadataKey1 = "metadataKey1";
    final String metadataKey2 = "metadataKey2";
    final String metadataKey3 = "metadataKey3";
    final String metadataValue1 = "metadataValue1";
    final String metadataValue2 = "metadataValue2";
    final String metadataValue3 = "metadataValue3";
    final String header1 = "header1";
    final String header2 = "header2";
    final String data1 = "data1";
    final String data2 = "data2";
    final String metadataFieldName = "customMetadataField";
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
        put("extractMetadata", String.valueOf(true));
        put("firstHeaderRow", String.valueOf(1));
        put("lastHeaderRow", String.valueOf(2));
        put("firstFooterRow", String.valueOf(5));
        put("lastFooterRow", String.valueOf(5));
        put("metadataFieldName", metadataFieldName);
        put("metadataCells", "A1:B1,A2:B2,A5:B5");

        put("columnNamesSelection", "customRowAsColumns");
        put("customColumnNamesRow", String.valueOf(3));
        put("lastDataRow", String.valueOf(4));
      }
    };
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateMetadataRows(metadataKey1, metadataValue1, metadataKey2, metadataValue2,
                                                           metadataKey3, metadataValue3, header1, header2,
                                                           data1, data2));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, FILE_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME,
                        GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(1, fileLines.size());

    String resultLine = fileLines.get(0);

    JsonParser jsonParser = new JsonParser();
    JsonElement rootElement = jsonParser.parse(resultLine);
    Assert.assertTrue(rootElement.isJsonObject());

    JsonObject rootObject = rootElement.getAsJsonObject();

    Assert.assertEquals(3, rootObject.entrySet().size());
    Assert.assertEquals(data1, rootObject.get(header1).getAsString());
    Assert.assertEquals(data2, rootObject.get(header2).getAsString());

    JsonObject metadataObject = rootObject.get(metadataFieldName).getAsJsonObject();

    // Image metadata entries: width, height, rotation
    Assert.assertEquals(3, metadataObject.entrySet().size());
    Assert.assertEquals(metadataValue1, metadataObject.get(metadataKey1).getAsString());
    Assert.assertEquals(metadataValue2, metadataObject.get(metadataKey2).getAsString());
    Assert.assertEquals(metadataValue3, metadataObject.get(metadataKey3).getAsString());
  }

  @Test
  public void testFormatting() throws Exception {
    final Double simpleNumberValue = 12d;
    final Double numberOfDays = 1d;
    final Double partOfDay = 0.5;
    final Double percent = 3d;
    final Double currency = 54d;
    Map<String, String> sourceProps = new HashMap<String, String>() {
      {
        putAll(getSheetsSourceMinimalDefaultConfigs());
      }
    };
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateFormattedRows(simpleNumberValue, numberOfDays, partOfDay, percent,
                                                            currency));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, SHEETS_SOURCE_STAGE_NAME, FILE_SINK_STAGE_NAME,
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME,
                        GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-" + testName.getMethodName());
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(1, fileLines.size());

    String resultLine = fileLines.get(0);

    JsonParser jsonParser = new JsonParser();
    JsonElement rootElement = jsonParser.parse(resultLine);
    Assert.assertTrue(rootElement.isJsonObject());

    JsonObject rootObject = rootElement.getAsJsonObject();

    Assert.assertEquals(5, rootObject.entrySet().size());
    Assert.assertEquals("12", rootObject.get("A").getAsString());
    Assert.assertEquals("31.12.1899", rootObject.get("B").getAsString());
    Assert.assertEquals("12:00:00", rootObject.get("C").getAsString());
    Assert.assertEquals("300%", rootObject.get("D").getAsString());
    Assert.assertEquals("$54", rootObject.get("E").getAsString());
  }

  private int getNonNullRowsCount(List<String> fileLines) {
    JsonParser jsonParser = new JsonParser();
    int notNullCount = 0;
    for (String line : fileLines) {
      JsonElement rootElement = jsonParser.parse(line);
      Assert.assertTrue(rootElement.isJsonObject());

      JsonObject rootObject = rootElement.getAsJsonObject();

      Assert.assertEquals(5, rootObject.entrySet().size());
      if (!rootObject.get("A").isJsonNull()) {
        notNullCount++;
      }
    }
    return notNullCount;
  }

  private Map<String, String> getSheetsSourceMinimalDefaultConfigs() {
    return new HashMap<String, String>() {
      {
        put("referenceName", "ref");
        put("directoryIdentifier", sourceFolderId);
        put("modificationDateRange", "lifetime");
        put("sheetsToPull", "all");

        put("extractMetadata", "false");
        put("metadataFieldName", "metadata");
        put("formatting", "formattedValues");
        put("skipEmptyData", "true");
        put("addNameFields", "false");
        put("columnNamesSelection", "noColumnNames");
        put("lastDataColumn", "5");
        put("lastDataRow", "5");
        put("readBufferSize", "5");

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
        put("format", "json");
        put("schema", fileSchema.toString());
      }
    };
  }

  private Map<String, String> getSheetsSinkMinimalDefaultConfigs() {
    return new HashMap<String, String>() {
      {
        put("referenceName", "refd");
        put("directoryIdentifier", sinkFolderId);
        put("spreadsheetName", DEFAULT_SPREADSHEET_NAME);
        put("sheetName", DEFAULT_SHEET_NAME);
        put("writeSchema", "false");
        put("threadsNumber", "1");
        put("maxBufferSize", "10");
        put("recordsQueueLength", "100");
        put("maxFlushInterval", "10");
        put("flushExecutionTimeout", "100");
        put("minPageExtensionSize", "100");
        put("mergeDataCells", "false");
        put("skipNameFields", "true");

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

  private static Spreadsheet getSpreadsheet(String spreadSheetId) throws IOException {
    Sheets.Spreadsheets.Get request = sheetsService.spreadsheets().get(spreadSheetId);
    request.setIncludeGridData(true);
    return request.execute();
  }

  private static List<RowData> generateSimpleRows(int numberOfRows, int numberOfColumns) {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < numberOfRows; i++) {
      List<CellData> row = new ArrayList<>();
      for (int j = 0; j < numberOfColumns; j++) {
        row.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(i + "" + j)));
      }
      rows.add(new RowData().setValues(row));
    }
    return rows;
  }

  private static List<RowData> generateRowsWithNames(int numberOfRowsPerName, int numberOfColumns, List<String> names) {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < numberOfRowsPerName; i++) {
      for (String name : names) {
        List<CellData> row = new ArrayList<>();
        for (int j = 0; j < numberOfColumns; j++) {
          row.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(name)));
        }
        rows.add(new RowData().setValues(row));
      }
    }
    return rows;
  }

  /**
   * | metadataKey1 | metadataValue1 |
   * | metadataKey2 | metadataValue2 |
   * | header1      | header2        |
   * | data 1       | data2          |
   * | metadataKey3 | metadataValue3 |
   *
   * @return list of populated rows.
   */
  private static List<RowData> generateMetadataRows(String metadataKey1, String metadataValue1,
                                                    String metadataKey2, String metadataValue2,
                                                    String metadataKey3, String metadataValue3,
                                                    String header1, String header2,
                                                    String data1, String data2) {
    List<RowData> rows = new ImmutableList.Builder<RowData>()
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataKey1)))
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataValue1)))
                                     .build()))
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataKey2)))
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataValue2)))
                                     .build()))
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(header1)))
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(header2)))
                                     .build()))
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(data1)))
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(data2)))
                                     .build()))
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataKey3)))
                                     .add(new CellData().setUserEnteredValue(
                                       new ExtendedValue().setStringValue(metadataValue3)))
                                     .build()))
      .build();
    return rows;
  }

  /**
   * | 2 | 12.09.2018 | 06:45:32 | 150% | $45 |
   *
   * @return list of populated rows.
   */
  private static List<RowData> generateFormattedRows(Double simpleNumberValue, Double numberOfDays,
                                                     Double partOfDay, Double percent,
                                                     Double currency) {
    List<RowData> rows = new ImmutableList.Builder<RowData>()
      .add(new RowData().setValues(new ImmutableList.Builder<CellData>()
                                     .add(new CellData()
                                            .setUserEnteredValue(new ExtendedValue().setNumberValue(simpleNumberValue)))
                                     .add(new CellData()
                                            .setUserEnteredValue(new ExtendedValue().setNumberValue(numberOfDays))
                                            .setUserEnteredFormat(new CellFormat().setNumberFormat(
                                              new NumberFormat().setType("DATE").setPattern("dd.MM.yyyy"))))
                                     .add(new CellData()
                                            .setUserEnteredValue(new ExtendedValue().setNumberValue(partOfDay))
                                            .setUserEnteredFormat(new CellFormat().setNumberFormat(
                                              new NumberFormat().setType("TIME").setPattern("HH:mm:ss"))))
                                     .add(new CellData()
                                            .setUserEnteredValue(new ExtendedValue().setNumberValue(percent))
                                            .setUserEnteredFormat(new CellFormat().setNumberFormat(
                                              new NumberFormat().setType("PERCENT").setPattern("0%"))))
                                     .add(new CellData()
                                            .setUserEnteredValue(new ExtendedValue().setNumberValue(currency))
                                            .setUserEnteredFormat(new CellFormat().setNumberFormat(
                                              new NumberFormat().setType("CURRENCY").setPattern("[$$]#0"))))
                                     .build()))
      .build();
    return rows;
  }

  private static void populateSpreadSheetWithSimpleRows(Sheets sheetsService, String fileId,
                                                        List<RowData> rowData) throws IOException {
    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    requestBody.setRequests(new ArrayList<>());

    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setFields("*");
    appendCellsRequest.setSheetId(getFirstAvailableSheetId(sheetsService, fileId));
    appendCellsRequest.setRows(rowData);

    requestBody.getRequests().add(new Request().setAppendCells(appendCellsRequest));

    Sheets.Spreadsheets.BatchUpdate request =
      sheetsService.spreadsheets().batchUpdate(fileId, requestBody);

    request.execute();
  }

  private static Integer getFirstAvailableSheetId(Sheets sheetsService, String fileId) throws IOException {
    Spreadsheet spreadsheet = sheetsService.spreadsheets().get(fileId).execute();
    return spreadsheet.getSheets().get(0).getProperties().getSheetId();
  }
}
