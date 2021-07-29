/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.api.client.util.Throwables;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.remote.dataset.AbstractDatasetApp;
import io.cdap.cdap.remote.dataset.cube.CubeDatasetApp;
import io.cdap.cdap.remote.dataset.cube.RemoteCube;
import io.cdap.cdap.remote.dataset.kvtable.KVTableDatasetApp;
import io.cdap.cdap.remote.dataset.kvtable.RemoteKeyValueTable;
import io.cdap.cdap.remote.dataset.table.RemoteTable;
import io.cdap.cdap.remote.dataset.table.TableDatasetApp;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.chaosmonkey.proto.ClusterDisruptor;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private static final String GCP_SERVICE_ACCOUNT_PATH = "google.application.credentials.path";
  private static final String GCP_SERVICE_ACCOUNT_BASE64_ENCODED = "google.application.credentials.base64.encoded";
  // Used for starting/stop await timeout.
  protected static final int PROGRAM_START_STOP_TIMEOUT_SECONDS =
    Integer.valueOf(System.getProperty("programTimeout", "120"));
  protected static final int POLL_INTERVAL_SECONDS =
    Integer.valueOf(System.getProperty("pollInterval", "5"));
  // Amount of time to wait for a program (i.e. Service, or Worker) to process its first event (upon startup).
  // For now, make it same as PROGRAM_START_STOP_TIMEOUT_SECONDS.
  protected static final int PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS = PROGRAM_START_STOP_TIMEOUT_SECONDS;

  protected static final NamespaceId TEST_NAMESPACE = getConfiguredNamespace();

  // avoid logging of HttpRequest's body by default, to avoid verbose logging
  private static final int logBodyLimit = Integer.valueOf(System.getProperty("logRequestBodyLimit", "0"));
  private final RESTClient restClient;
  protected DisruptorFactory disruptor;

  private com.google.auth.oauth2.AccessToken oauthToken;

  @After
  public void stopDisruptor() {
    if (disruptor != null) {
      disruptor.disruptorStop();
    }
  }

  public AudiTestBase() {
    restClient = new RESTClient(getClientConfig());
    restClient.addListener(createRestClientListener());

    disruptor = new DisruptorFactory();
  }

  @Nullable
  @Override
  protected AccessToken getAccessToken() {
    // Use basic auth if its defined (to maintain backwards compatibility)
    String name = System.getProperty("cdap.username");
    String password = System.getProperty("cdap.password");
    try {
      if (name != null && password != null) {
        return fetchAccessToken(name, password);
      }
    } catch (IOException | TimeoutException | InterruptedException e) {
      Throwables.propagate(e);
    }

    try {
      // Get service account contents either from file or base64 encoded string
      String serviceAccountCredentials = getServiceAccount();
      if (serviceAccountCredentials == null) {
        return null;
      }
      GoogleCredentials creds = ServiceAccountCredentials
        .fromStream(new ByteArrayInputStream(serviceAccountCredentials.getBytes(StandardCharsets.UTF_8)))
        .createScoped("https://www.googleapis.com/auth/cloud-platform");
      Date now = new Date(System.currentTimeMillis());
      // Refresh the token if the oauthToken is not already defined or it is expired
      if (oauthToken == null || now.after(oauthToken.getExpirationTime())) {
        oauthToken = creds.refreshAccessToken();
      }

      return new AccessToken(oauthToken.getTokenValue(), oauthToken.getExpirationTime().getTime(), "Bearer");
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return null;
  }

  private String getServiceAccount() throws IOException {
    String serviceAccountPath = System.getProperty(GCP_SERVICE_ACCOUNT_PATH);
    String serviceAccountEncoded = System.getProperty(GCP_SERVICE_ACCOUNT_BASE64_ENCODED);

    if (serviceAccountEncoded != null) {
      return Bytes.toString(Base64.getDecoder().decode(serviceAccountEncoded));
    }
    if (serviceAccountPath != null) {
      return new String(Files.readAllBytes(Paths.get(serviceAccountPath)), StandardCharsets.UTF_8);
    }
    return null;
  }

  // should always use this RESTClient because it has listeners which log upon requests made and responses received.
  @Override
  protected RESTClient getRestClient() {
    return restClient;
  }

  // constructs a RestClient.Listener with logging upon each request
  protected RESTClient.Listener createRestClientListener() {
    return new RESTClient.Listener() {
      @Override
      public void onRequest(HttpRequest httpRequest, int i) {
        try {
          ContentProvider<? extends InputStream> inputSupplier = httpRequest.getBody();
          String body = null;
          if (inputSupplier != null) {
            try (InputStream is = inputSupplier.getInput()) {
              body = CharStreams.toString(new InputStreamReader(is, StandardCharsets.UTF_8));
            }
          }

          if (logBodyLimit > 0) {
            if (body != null && body.length() >= logBodyLimit) {
              body = body.substring(0, logBodyLimit) + " ... [TRIMMED]";
            }
            LOG.info("Making request: {} {} - body: {}", httpRequest.getMethod(), httpRequest.getURL(), body);
          } else {
            // omit the body from being logged, if user doesn't explicitly request it
            LOG.info("Making request: {} {}", httpRequest.getMethod(), httpRequest.getURL());
          }
        } catch (IOException e) {
          LOG.error("Failed to get body from http request: {} {}", httpRequest.getMethod(), httpRequest.getURL(), e);
        }
      }

      @Override
      public void onResponse(HttpRequest httpRequest, HttpResponse httpResponse, int i) {
        LOG.info("Received response: [{}] Response Body: {}",
                 httpResponse.getResponseCode(), httpResponse.getResponseBodyAsString());
      }
    };
  }

  protected ClusterDisruptor getClusterDisruptor() throws Exception {
    if (disruptor.getClusterDisruptor() == null) {
      disruptor.disruptorStart();
    }
    return disruptor.getClusterDisruptor();
  }

  protected void checkMetricAtLeast(final Map<String, String> tags, final String metric,
                                    long expectedCount, int timeOutSeconds) throws Exception {
    Tasks.waitFor(true, () -> getMetricValue(tags, metric) >= expectedCount,
                  timeOutSeconds, TimeUnit.SECONDS, 2, TimeUnit.SECONDS);
  }

  protected void checkMetric(final Map<String, String> tags, final String metric,
                             long expectedCount, int timeOutSeconds) throws Exception {
    Tasks.waitFor(expectedCount, () -> getMetricValue(tags, metric),
                  timeOutSeconds, TimeUnit.SECONDS, 2, TimeUnit.SECONDS);
  }

  protected long getMetricValue(Map<String, String> tags, String metric) throws Exception {
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

  protected List<RunRecord> getRunRecords(int expectedSize, final ProgramClient programClient, final ProgramId program,
                                          final String status, final long startTime, final long endTime)
    throws Exception {
    final List<RunRecord> runRecords = new ArrayList<>();
    // Tasks.waitFor can be removed when CDAP-3656 is fixed
    Tasks.waitFor(expectedSize, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        runRecords.clear();
        runRecords.addAll(
          programClient.getProgramRuns(program, status, startTime, endTime, Integer.MAX_VALUE));
        return runRecords.size();
      }
    }, 30, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
    return runRecords;
  }

  protected void assertRuns(int count, ProgramClient programClient,
                            ProgramRunStatus expectedStatus, ProgramId... programIds) throws Exception {
    for (ProgramId programId : programIds) {
      List<RunRecord> runRecords =
        getRunRecords(count, programClient, programId, expectedStatus.name(), 0, Long.MAX_VALUE);
      Assert.assertEquals(String.format("Number of runs for program %s is not equal to the expected", programId),
                          count, runRecords.size());
      for (RunRecord runRecord : runRecords) {
        Assert.assertEquals(String.format("Run status of program %s is not equal to the expected", programId),
                            expectedStatus, runRecord.getStatus());
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected final <T extends DatasetAdmin> T addDatasetInstance(NamespaceId namespace,
                                                                String datasetTypeName, String datasetInstanceName,
                                                                DatasetProperties props) throws Exception {
    DatasetId datasetInstance = namespace.dataset(datasetInstanceName);
    DatasetInstanceConfiguration dsConf = new DatasetInstanceConfiguration(datasetTypeName, props.getProperties());

    DatasetClient datasetClient = getDatasetClient();
    datasetClient.create(datasetInstance, dsConf);
    return (T) new RemoteDatasetAdmin(datasetClient, datasetInstance, dsConf);
  }

  // TODO: improve the following getXDataset methods. Eventually, move them into IntegrationTestBase.
  // Consider whether they should should have behavior of createIfNotExists.
  protected DataSetManager<Table> getTableDataset(String datasetName) throws Exception {
    return getTableDataset(datasetName, getRestClient(), getClientConfig());
  }

  protected DataSetManager<Table> getTableDataset(String datasetName, RESTClient restClient,
                                                  ClientConfig clientConfig) throws Exception {
    return getTableDataset(TEST_NAMESPACE, datasetName, restClient, clientConfig);
  }

  protected DataSetManager<Table> getTableDataset(NamespaceId namespace, String datasetName, RESTClient restClient,
                                                  ClientConfig clientConfig) throws Exception {
    return wrap(new RemoteTable(deployServiceForDataset(namespace, TableDatasetApp.class,
                                                        datasetName, getTestManager(clientConfig, restClient)),
                                restClient, clientConfig));
  }

  protected DataSetManager<KeyValueTable> getKVTableDataset(String datasetName) throws Exception {
    return wrap(new RemoteKeyValueTable(
      deployServiceForDataset(TEST_NAMESPACE, KVTableDatasetApp.class, datasetName),
      getRestClient(), getClientConfig()));
  }

  protected DataSetManager<KeyValueTable> getKVTableDataset(DatasetId datasetId) throws Exception {
    return wrap(new RemoteKeyValueTable(deployServiceForDataset(datasetId.getParent(),
                                                                KVTableDatasetApp.class, datasetId.getDataset()),
                                        getRestClient(), getClientConfig()));
  }

  protected DataSetManager<Cube> getCubeDataset(String datasetName) throws Exception {
    return wrap(new RemoteCube(deployServiceForDataset(TEST_NAMESPACE, CubeDatasetApp.class, datasetName),
                               getRestClient(), getClientConfig()));
  }

  protected void startAndWaitForRun(ProgramManager<?> programManager, ProgramRunStatus runStatus)
    throws InterruptedException, ExecutionException, TimeoutException {
    startAndWaitForRun(programManager, runStatus, Collections.emptyMap());
  }

  protected void startAndWaitForRun(ProgramManager<?> programManager, ProgramRunStatus runStatus,
                                    Map<String, String> runtimeArgs)
    throws InterruptedException, ExecutionException, TimeoutException {
    startAndWaitForRun(programManager, runStatus, runtimeArgs, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  protected void startAndWaitForRun(ProgramManager<?> programManager, ProgramRunStatus runStatus,
                                    long waitTime, TimeUnit waitUnit)
    throws InterruptedException, ExecutionException, TimeoutException {
    startAndWaitForRun(programManager, runStatus, Collections.emptyMap(), waitTime, waitUnit);
  }

  protected void startAndWaitForRun(ProgramManager<?> programManager, ProgramRunStatus runStatus,
                                    Map<String, String> runtimeArgs, long waitTime, TimeUnit waitUnit)
    throws InterruptedException, ExecutionException, TimeoutException {
    if (runStatus.isUnsuccessful()) {
      programManager.startAndWaitForRun(runtimeArgs, runStatus, waitTime, waitUnit,
                                        POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
    } else {
      if (programManager instanceof AbstractProgramManager) {
        ((AbstractProgramManager<?>) programManager).startAndWaitForRun(runtimeArgs, runStatus,
                                                                        r -> r != null && r.isUnsuccessful(),
                                                                        waitTime, waitUnit,
                                                                        POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);
      } else {
        programManager.startAndWaitForGoodRun(runtimeArgs, runStatus, waitTime, waitUnit);
      }
    }
  }

  // ensures that the Service for the dataset is deployed and running
  // returns its baseURL
  private URL deployServiceForDataset(NamespaceId namespace, Class<? extends Application> applicationClz,
                                      String datasetName) throws Exception {
    return deployServiceForDataset(namespace, applicationClz, datasetName, getTestManager());
  }

  // ensures that the Service for the dataset is deployed and running
  // returns its baseURL
  private URL deployServiceForDataset(NamespaceId namespace, Class<? extends Application> applicationClz,
                                      String datasetName, TestManager testManager) throws Exception {
    ApplicationManager appManager =
      testManager.deployApplication(namespace, applicationClz, new AbstractDatasetApp.DatasetConfig(datasetName));
    ServiceManager serviceManager =
      appManager.getServiceManager(AbstractDatasetApp.DatasetService.class.getSimpleName());

    // start the service and wait until it becomes reachable
    if (!serviceManager.isRunning()) {
      startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    }
    return serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  // wraps a Dataset within a DatasetManager
  private <T> DataSetManager<T> wrap(final Dataset dataset) {
    return new DataSetManager<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public T get() {
        return (T) dataset;
      }

      @Override
      public void flush() {
        // no need to do anything, because each operation on the dataset happens within its own transaction
        // because it is in one HttpHandler method call
      }
    };
  }
}
