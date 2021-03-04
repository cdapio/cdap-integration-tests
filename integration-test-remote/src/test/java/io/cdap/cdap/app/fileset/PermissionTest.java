/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.fileset;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.suite.category.MapR5Incompatible;
import io.cdap.cdap.test.suite.category.SDKIncompatible;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests the permissions and group of files created through ((time-)partitioned) file sets.
 */
@Category({
  // Do not run the tests on SDK because the local MR runner does not appear to obey the umask
  SDKIncompatible.class,
  // Do not run for MapR, CDAP-8721, CDAP-8723: in MapR fs the permissions and group of the parent are not
  // inherited by the child.
  MapR5Incompatible.class
})
// TODO: (CDAP-17747) Re-enable this test once its been fixed
@Ignore
public class PermissionTest extends AudiTestBase {

  private static final Gson GSON = new Gson();
  private static final List<String> DATA_LIST =
    Lists.newArrayList("Hello World", "Hello Hello World", "World Hello");
  private static final String DATA_UPLOAD = Joiner.on("\n").join(DATA_LIST);
  private static final Logger LOG = LoggerFactory.getLogger(PermissionTest.class);
  @SuppressWarnings("OctalInteger")
  private static final int REMOVE_X_MASK = 0666;

  @Test
  public void test() throws Exception {

    ApplicationManager applicationManager = deployApplication(TEST_NAMESPACE, PermissionTestApp.class);
    // 1. start service and wait for it to come up
    ServiceManager serviceManager = applicationManager.getServiceManager(PermissionTestApp.SERVICE).start();
    URL serviceURL = serviceManager.getServiceURL(300, TimeUnit.SECONDS);

    // 2. call the service to find out what groups are available, and assign them to datasets
    HttpResponse response = getRestClient().execute(HttpRequest.get(new URL(serviceURL, "groups")).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    String[] groups = GSON.fromJson(response.getResponseBodyAsString(), String[].class);
    LOG.info("App user's groups: " + Arrays.toString(groups));

    int idx = 0;
    String fsGroup = groups[idx++];
    String pfsGroup = idx < groups.length ? groups[idx++] : fsGroup;
    String tpfsGroup = idx < groups.length ? groups[idx++] : pfsGroup;
    String inputGroup = idx < groups.length ? groups[idx] : tpfsGroup;

    int inputPerms = Integer.parseInt("771", 8);
    int fsPerms = Integer.parseInt("720", 8);
    int pfsPerms = Integer.parseInt("724", 8);
    int tpfsPerms = Integer.parseInt("762", 8);

    // 3. create the input dataset and validate its group and permissions
    getTestManager().addDatasetInstance(FileSet.class.getName(), TEST_NAMESPACE.dataset(PermissionTestApp.INPUT),
                                        FileSetProperties.builder()
                                          .setInputFormat(TextInputFormat.class)
                                          .setFileGroup(inputGroup)
                                          .setFilePermissions(Integer.toOctalString(inputPerms))
                                          .build());
    validateGroupAndPermissions(serviceURL, PermissionTestApp.INPUT, inputPerms, inputGroup);

    // 4. upload a file into the input dataset - all mapreduce will read this same input
    // and validate that all files and directories have the correct group and permissions
    response = getRestClient().execute(HttpRequest.put(
      new URL(serviceURL, "input")).withBody(DATA_UPLOAD).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    validateGroupAndPermissions(serviceURL, PermissionTestApp.INPUT, inputPerms, inputGroup);

    // 5. create three output datasets and validate their permissions
    getTestManager().addDatasetInstance(FileSet.class.getName(), TEST_NAMESPACE.dataset(PermissionTestApp.FS),
                                        FileSetProperties.builder()
                                          .setOutputFormat(TextOutputFormat.class)
                                          .setFileGroup(fsGroup)
                                          .setFilePermissions(Integer.toOctalString(fsPerms))
                                          .build());
    getTestManager().addDatasetInstance(PartitionedFileSet.class.getName(),
                                        TEST_NAMESPACE.dataset(PermissionTestApp.PFS),
                                        PartitionedFileSetProperties.builder()
                                          .setPartitioning(
                                            Partitioning.builder().addStringField("x").addStringField("y").build())
                                          .setOutputFormat(TextOutputFormat.class)
                                          .setFileGroup(pfsGroup)
                                          .setFilePermissions(Integer.toOctalString(pfsPerms))
                                          .build());
    getTestManager().addDatasetInstance(TimePartitionedFileSet.class.getName(),
                                        TEST_NAMESPACE.dataset(PermissionTestApp.TPFS),
                                        FileSetProperties.builder()
                                          .setOutputFormat(TextOutputFormat.class)
                                          .setFileGroup(tpfsGroup)
                                          .setFilePermissions(Integer.toOctalString(tpfsPerms))
                                          .build());
    validateGroupAndPermissions(serviceURL, PermissionTestApp.FS, fsPerms, fsGroup);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.PFS, pfsPerms, pfsGroup);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.TPFS, tpfsPerms, tpfsGroup);

    long firstTime = System.currentTimeMillis();
    long secondTime = firstTime + TimeUnit.HOURS.toMillis(1);
    long thirdTime = secondTime + TimeUnit.HOURS.toMillis(1);
    long fourthTime = thirdTime + TimeUnit.HOURS.toMillis(1);
    String firstPath = "first/path";
    String secondPath = "second/path";
    String thirdPath = "third/path";
    String fourthPath = "fourth/path";

    MapReduceManager mrManager = applicationManager.getMapReduceManager(PermissionTestApp.MAPREDUCE);

    // 6. run the mapreduce three times, each writing to one dataset, then validate permissions
    // writes to file set
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED, ImmutableMap.of("path", firstPath),
                       300, TimeUnit.SECONDS);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.FS, fsPerms, fsGroup);

    // writes to TPFS
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED, ImmutableMap.of("time", String.valueOf(firstTime)),
                       300, TimeUnit.SECONDS);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.TPFS, tpfsPerms, tpfsGroup);

    // write to PFS with dynamic partitioning // dynamic output to PFS
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED, ImmutableMap.of("key", "1"), 300, TimeUnit.SECONDS);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.PFS, pfsPerms, pfsGroup);

    // 7. write to all datasets with multi-output. They should get the default permissions
    // to find out what they are, create a file set with default permissions and query the permissions
    getTestManager().addDatasetInstance(FileSet.class.getName(), TEST_NAMESPACE.dataset(PermissionTestApp.TEMP));
    response = getRestClient().execute(HttpRequest.get(
      new URL(serviceURL, "list/" + PermissionTestApp.TEMP)).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    PermissionTestApp.Listing listing = GSON.fromJson(response.getResponseBodyAsString(),
                                                      PermissionTestApp.Listing.class);
    LOG.debug("Listing: " + new GsonBuilder().setPrettyPrinting().create().toJson(listing));
    int defaultPerms = FileUtils.parsePermissions(listing.getPermission());
    LOG.info("Default permissions are: {}", Integer.toOctalString(defaultPerms));

    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED,
                       ImmutableMap.of("key", "2", "path", secondPath, "time", String.valueOf(secondTime)),
                       300, TimeUnit.SECONDS);
    // validate only the sub-paths written by this run
    validateGroupAndPermissions(serviceURL, PermissionTestApp.FS, defaultPerms, fsGroup, "path=" + secondPath);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.PFS, defaultPerms, pfsGroup, "key=2");
    validateGroupAndPermissions(serviceURL, PermissionTestApp.TPFS, defaultPerms, tpfsGroup, "time=" + secondTime);

    // 8. write to all datasets with multi-output, this time with explicit umask
    int multiPerms = Integer.parseInt("700", 8);
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED,
                       ImmutableMap.of("key", "3", "path", thirdPath, "time", String.valueOf(thirdTime),
                                       "umask", "077"),
                       300, TimeUnit.SECONDS);
    // validate only the sub-paths written by this run
    validateGroupAndPermissions(serviceURL, PermissionTestApp.FS, multiPerms, fsGroup, "path=" + thirdPath);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.PFS, multiPerms, pfsGroup, "key=3");
    validateGroupAndPermissions(serviceURL, PermissionTestApp.TPFS, multiPerms, tpfsGroup, "time=" + thirdTime);

    // 9. write to all datasets with multi-output, this time configure permissions as runtime args
    int rtPerms = Integer.parseInt("777", 8);
    startAndWaitForRun(mrManager, ProgramRunStatus.COMPLETED,
                       ImmutableMap.of("key", "4", "path", fourthPath, "time", String.valueOf(fourthTime),
                                       FileSetProperties.PROPERTY_FILES_PERMISSIONS, Integer.toOctalString(rtPerms)),
                       300, TimeUnit.SECONDS);
    // validate only the sub-paths written by this run
    validateGroupAndPermissions(serviceURL, PermissionTestApp.FS, rtPerms, fsGroup, "path=" + fourthPath);
    validateGroupAndPermissions(serviceURL, PermissionTestApp.PFS, rtPerms, pfsGroup, "key=4");
    validateGroupAndPermissions(serviceURL, PermissionTestApp.TPFS, rtPerms, tpfsGroup, "time=" + fourthTime);
  }

  private void validateGroupAndPermissions(URL serviceURL, String dataset,
                                           int expectedPerms, String expectedGroup)
    throws IOException, UnauthorizedException, UnauthenticatedException {
    validateGroupAndPermissions(serviceURL, dataset, expectedPerms, expectedGroup, null);
  }

  private void validateGroupAndPermissions(URL serviceURL, String dataset,
                                           int expectedPerms, String expectedGroup, @Nullable String query)
    throws IOException, UnauthorizedException, UnauthenticatedException {
    query = query == null ? "" : "?" + query;
    HttpResponse response = getRestClient().execute(HttpRequest.get(
      new URL(serviceURL, "list/" + dataset + query)).build(), getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());
    PermissionTestApp.Listing listing = GSON.fromJson(response.getResponseBodyAsString(),
                                                      PermissionTestApp.Listing.class);
    LOG.debug("Listing: " + new GsonBuilder().setPrettyPrinting().create().toJson(listing));
    validateGroupAndPermissions(listing, expectedPerms, expectedGroup);
  }

  private void validateGroupAndPermissions(PermissionTestApp.Listing listing,
                                           int expectedPermissions, String expectedGroup) {
    Assert.assertEquals(String.format("Expected group '%s' for path %s but found '%s'",
                                      expectedGroup, listing.getPath(), listing.getGroup()),
                        expectedGroup, listing.getGroup());
    int actualPermissions = FileUtils.parsePermissions(listing.getPermission());
    if (listing.getChildren() == null) {
      // for plain files, Hadoop will sometimes remove the execute bits
      Assert.assertEquals(String.format("Expected permissions '%s' (ignoring x bits) for path %s but found '%s'",
                                        Integer.toOctalString(expectedPermissions), listing.getPath(),
                                        Integer.toOctalString(actualPermissions)),
                          expectedPermissions & REMOVE_X_MASK, actualPermissions & REMOVE_X_MASK);
    } else {
      Assert.assertEquals(String.format("Expected permissions '%s' (including x bits) for path %s but found '%s'",
                                        Integer.toOctalString(expectedPermissions), listing.getPath(),
                                        Integer.toOctalString(actualPermissions)),
                          expectedPermissions, actualPermissions);
      for (PermissionTestApp.Listing child : listing.getChildren()) {
        validateGroupAndPermissions(child, expectedPermissions, expectedGroup);
      }
    }
  }
}
