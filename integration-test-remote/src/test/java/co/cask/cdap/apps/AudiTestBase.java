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

import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  protected static final Id.Namespace TEST_NAMESPACE = Constants.DEFAULT_NAMESPACE_ID;
  private static final long SERVICE_CHECK_TIMEOUT = TimeUnit.MINUTES.toSeconds(10);
  public static final NamespaceMeta DEFAULT =
    new NamespaceMeta.Builder().setName(Constants.DEFAULT_NAMESPACE_ID).setDescription("Default Namespace").build();

  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private final RESTClient restClient;

  @Override
  public void setUp() throws Exception {
    checkSystemServices();
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    // stop everything in default namespace
    getProgramClient().stopAll();
    NamespaceClient namespaceClient = getNamespaceClient();
    for (NamespaceMeta namespaceMeta : namespaceClient.list()) {
      namespaceClient.delete(namespaceMeta.getName());
    }
    super.tearDown();
  }

  private boolean allSystemServicesOk() throws IOException, UnauthorizedException {
    for (Map.Entry<String, String> entry : getMonitorClient().getAllSystemServiceStatus().entrySet()) {
      // Exclude explore service, since it does not work on secured cluster prior to 3.1 see CDAP-1905
      if (entry.getKey().equals("explore.service")) {
        continue;
      }
      if (!"OK".equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  // TODO Remove this code if we decide to backport IntegrationTestBase from CDAP repo
  protected void checkSystemServices() throws TimeoutException {
    Callable<Boolean> cdapAvailable = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // first wait for all system services to be 'OK'
        if (!allSystemServicesOk()) {
          return false;
        }
        // Check that the dataset service is up, and also that the default namespace exists
        // Using list and checking that the only namespace to exist is default, as opposed to using get()
        // so we don't have to unnecessarily add a try-catch for NamespaceNotFoundException, since that exception is
        // not handled in checkServicesWithRetry.
        List<NamespaceMeta> namespaces = getNamespaceClient().list();
        return namespaces.size() == 1 && DEFAULT.equals(namespaces.get(0));
      }
    };

    checkServicesWithRetry(cdapAvailable, "CDAP Services are not available");

    LOG.info("CDAP Services are up and running!");
  }

  protected MonitorClient getMonitorClient() {
    return new MonitorClient(getClientConfig(), getRestClient());
  }

  protected NamespaceClient getNamespaceClient() {
    return new NamespaceClient(getClientConfig(), getRestClient());
  }


  private void checkServicesWithRetry(Callable<Boolean> callable,
                                      String exceptionMessage) throws TimeoutException {
    int numSecs = 0;
    do {
      try {
        numSecs++;
        if (callable.call()) {
          return;
        }
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException | IOException e) {
        // We want to suppress and retry on InterruptedException or IOException
      } catch (Throwable e) {
        // Also suppress and retry if the root cause is InterruptedException or IOException
        Throwable rootCause = Throwables.getRootCause(e);
        if (!(rootCause instanceof InterruptedException || rootCause instanceof IOException)) {
          // Throw if root cause is any other exception e.g. UnauthorizedException
          throw Throwables.propagate(rootCause);
        }
      }
    } while (numSecs <= SERVICE_CHECK_TIMEOUT);

    // when we have passed the timeout and the check for services is not successful
    throw new TimeoutException(exceptionMessage);
  }

  public AudiTestBase() {
    restClient = createRestClient();
  }

  // should always use this RESTClient because it has listeners which log upon requests made and responses received.
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
    MetricQueryResult metricQueryResult = new MetricsClient(getClientConfig(),
                                                            getRestClient()).query(tags, metric, null);
    MetricQueryResult.TimeSeries[] series = metricQueryResult.getSeries();
    if (series.length == 0) {
      return 0;
    }
    Preconditions.checkState(series.length == 1, "Metric TimeSeries has more than one TimeSeries: {}", series);
    MetricQueryResult.TimeValue[] timeValues = series[0].getData();
    Preconditions.checkState(timeValues.length == 1, "Metric TimeValues has more than one TimeValue: {}", timeValues);
    return timeValues[0].getValue();
  }

  // {@param} expectedStatus can be null if count is 0
  protected void assertRuns(int count, ProgramClient programClient,
                            @Nullable ProgramRunStatus expectedStatus,  Id.Program... programIds) throws Exception {
    for (Id.Program programId : programIds) {
      List<RunRecord> runRecords =
        programClient.getAllProgramRuns(programId.getApplicationId(), programId.getType(), programId.getId(),
                                        0, Long.MAX_VALUE, Integer.MAX_VALUE);
      Assert.assertEquals(count, runRecords.size());
      for (RunRecord runRecord : runRecords) {
        Assert.assertEquals(expectedStatus, runRecord.getStatus());
      }
    }
  }

  protected HttpResponse retryRestCalls(final int expectedStatusCode, final HttpRequest request,
                                        final RESTClient restClient, long timeout, TimeUnit timeoutUnit,
                                        long pollInterval, TimeUnit pollIntervalUnit) throws Exception {
    final AtomicReference<HttpResponse> ref = new AtomicReference<>();
    Tasks.waitFor(expectedStatusCode, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = restClient.execute(request, getClientConfig().getAccessToken());
        ref.set(response);
        return response.getResponseCode();
      }
    }, timeout, timeoutUnit, pollInterval, pollIntervalUnit);
    Preconditions.checkNotNull(ref.get(), "No Httprequest was attempted.");
    return ref.get();
  }
}
