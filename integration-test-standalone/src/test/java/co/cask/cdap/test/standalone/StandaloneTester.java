/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.standalone;

import co.cask.cdap.StandaloneMain;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.ETLBatchTemplate;
import co.cask.cdap.template.etl.realtime.ETLRealtimeTemplate;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * This class helps writing tests that needs {@link StandaloneMain} up and running.
 */
public class StandaloneTester extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneTester.class);

  private final TemporaryFolder tmpFolder = new TemporaryFolder();
  private Multimap<Class<?>, Class<?>> templatesToPlugins;
  private CConfiguration cConf;
  private StandaloneMain standaloneMain;

  public StandaloneTester(Multimap<Class<?>, Class<?>> map) {
    this.templatesToPlugins = map;
  }

  @Override
  protected void before() throws Throwable {
    tmpFolder.create();

    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.APP_TEMPLATE_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Router.ADDRESS, getLocalHostname());
    cConf.setInt(Constants.Router.ROUTER_PORT, Networks.getRandomPort());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(StandaloneMain.DISABLE_UI, true);

    this.cConf = cConf;

    if (!templatesToPlugins.keySet().isEmpty()) {
      LocationFactory lf = new LocalLocationFactory(tmpFolder.newFolder());
      File adapterDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
      for (Class<?> cls : templatesToPlugins.keySet()) {
        setupTemplates(adapterDir, lf, cls);
      }
      for (Class<?> cls : templatesToPlugins.keySet()) {
        String templateName;
        if (ETLRealtimeTemplate.class.equals(cls)) {
          templateName = "ETLRealtime";
        } else if (ETLBatchTemplate.class.equals(cls)) {
          templateName = "ETLBatch";
        } else {
          throw new Exception("");
        }
        Id.ApplicationTemplate applicationTemplate = Id.ApplicationTemplate.from(templateName);
        Collection<Class<?>> values = templatesToPlugins.get(cls);
        if (!values.isEmpty()) {
          Class<?>[] classes = values.toArray(new Class<?>[0]);
          Class<?> clz = classes[0];
          Class<?>[] rest = Arrays.copyOfRange(classes, 1, classes.length);
          setupPlugins(lf, applicationTemplate, "integration-test-1.0.0.jar", clz, rest);
        }
      }
    }

    // Start standalone
    standaloneMain = StandaloneMain.create(cConf, new Configuration());
    standaloneMain.startUp();
    try {
      waitForStandalone();
    } catch (Throwable t) {
      standaloneMain.shutDown();
      throw t;
    }
  }

  @Override
  protected void after() {
    try {
      if (standaloneMain != null) {
        standaloneMain.shutDown();
      }
    } finally {
      tmpFolder.delete();
    }
  }

  /**
   * Returns the {@link CConfiguration} used by the standalone instance.
   */
  public CConfiguration getConfiguration() {
    Objects.requireNonNull(cConf, "StandaloneTester hasn't been initialized");
    return cConf;
  }

  /**
   * Returns the base URI for making REST calls to the standalone instance.
   */
  public URI getBaseURI() {
    return URI.create(String.format("http://%s:%d/",
                                    getConfiguration().get(Constants.Router.ADDRESS),
                                    getConfiguration().getInt(Constants.Router.ROUTER_PORT)));
  }

  private String getLocalHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Unable to resolve localhost", e);
      return "127.0.0.1";
    }
  }

  private void waitForStandalone() throws Exception {
    final URL url = getBaseURI().resolve("/ping").toURL();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        try {
          return urlConn.getResponseCode() == HttpURLConnection.HTTP_OK;
        } finally {
          urlConn.disconnect();
        }
      }
    }, 30, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private void setupTemplates(File adapterDir, LocationFactory locationFactory, Class<?> clz) throws IOException {
    Location adapterJar = AppJarHelper.createDeploymentJar(locationFactory, clz);
    File destination =  new File(String.format("%s/%s", adapterDir.getAbsolutePath(), adapterJar.getName()));
    try (InputStream input = adapterJar.getInputStream()) {
      Files.copy(input, destination.toPath());
    }
  }

  private void setupPlugins(LocationFactory locationFactory, Id.ApplicationTemplate templateId, String jarName,
                            Class<?> pluginClz, Class<?>... classes) throws Exception {
    System.out.println("print here");
    System.out.println(pluginClz);
    System.out.println(classes[0]);
    File pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));
    Manifest manifest = new Manifest();
    Set<String> exportPackages = new HashSet<>();
    for (Class<?> cls : Iterables.concat(ImmutableList.of(pluginClz), ImmutableList.copyOf(classes))) {
      exportPackages.add(cls.getPackage().getName());
    }

    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    Location pluginJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClz, classes);
    File templatePluginDir = new File(pluginDir, templateId.getId());
    DirUtils.mkdirs(templatePluginDir);
    File destination = new File(templatePluginDir, jarName);
    com.google.common.io.Files.copy(Locations.newInputSupplier(pluginJar), destination);
  }
}
