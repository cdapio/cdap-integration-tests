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

package co.cask.cdap.apps;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private final RESTClient restClient;

  public AudiTestBase() {
    restClient = createRestClient();
  }

  @Before
  public void before() throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return new MonitorClient(getClientConfig(), getRestClient()).allSystemServicesOk();
      }
    }, 20, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    assertUnrecoverableResetEnabled();
  }

  protected ApplicationManager deployApplication(String applicationJarName) throws Exception {
    return deployApplication(getClientConfig().getNamespace(), applicationJarName);
  }

  @SuppressWarnings("unchecked")
  protected ApplicationManager deployApplication(Id.Namespace namespace, String applicationJarName) throws Exception {
    URL jarURL = getClass().getClassLoader().getResource(applicationJarName);
    Preconditions.checkNotNull(jarURL, "Application jar (%s) missing from classpath. " +
      "May need to run 'mvn process-test-resources' if running test from IDE.", applicationJarName);

    // Expand the jar and create a classloader from the directory
    File dir = TEMP_FOLDER.newFolder();
    BundleJarUtil.unpackProgramJar(Resources.newInputStreamSupplier(jarURL), dir);
    ProgramClassLoader classLoader = ProgramClassLoader.create(dir, getClass().getClassLoader());
    String mainClass = classLoader.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
    return deployApplication(namespace, (Class<? extends Application>) classLoader.loadClass(mainClass));
  }


  private void assertUnrecoverableResetEnabled() throws IOException, UnauthorizedException {
    ConfigEntry configEntry = getMetaClient().getCDAPConfig().get(Constants.Dangerous.UNRECOVERABLE_RESET);
    Preconditions.checkNotNull(configEntry,
                               "Missing key from CDAP Configuration: {}", Constants.Dangerous.UNRECOVERABLE_RESET);
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "UnrecoverableReset not enabled.");
  }

  // should always use this RESTClient because it has listeners which log upon requests made and responses received.
  @Override
  protected RESTClient getRestClient() {
    return restClient;
  }

  // constructs a RestClient with logging upon each request
  private RESTClient createRestClient() {
    RESTClient restClient = new RESTClient(getClientConfig());
    restClient.addListener(new RESTClient.Listener() {
      @Override
      public void onRequest(HttpRequest httpRequest, int i) {
        try {
          InputSupplier<? extends InputStream> inputSupplier = httpRequest.getBody();
          String body = null;
          if (inputSupplier != null) {
            body = CharStreams.toString(CharStreams.newReaderSupplier(inputSupplier, Charsets.UTF_8));
          }
          LOG.info("Making request: {} {} - body: {}", httpRequest.getMethod(), httpRequest.getURL(), body);
        } catch (IOException e) {
          LOG.error("Failed to get body from http request: {} {}", httpRequest.getMethod(), httpRequest.getURL(), e);
        }
      }

      @Override
      public void onResponse(HttpRequest httpRequest, HttpResponse httpResponse, int i) {
        LOG.info("Received response: [{}] Response Body: {}",
                 httpResponse.getResponseCode(), httpResponse.getResponseBodyAsString());
      }
    });
    return restClient;
  }

  protected void checkMetric(final Map<String, String> tags, final String metric,
                             long expectedCount, int timeOutSeconds) throws Exception {
    Tasks.waitFor(expectedCount, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getMetricValue(tags, metric);
      }
    }, timeOutSeconds, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private long getMetricValue(Map<String, String> tags, String metric) throws Exception {
    MetricQueryResult metricQueryResult = getMetricsClient().query(tags, metric);
    MetricQueryResult.TimeSeries[] series = metricQueryResult.getSeries();
    if (series.length == 0) {
      return 0;
    }
    Preconditions.checkState(series.length == 1, "Metric TimeSeries has more than one TimeSeries: {}", series);
    MetricQueryResult.TimeValue[] timeValues = series[0].getData();
    Preconditions.checkState(timeValues.length == 1, "Metric TimeValues has more than one TimeValue: {}", timeValues);
    return timeValues[0].getValue();
  }
}
