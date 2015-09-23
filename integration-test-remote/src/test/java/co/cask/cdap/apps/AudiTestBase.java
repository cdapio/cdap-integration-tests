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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.remote.dataset.RemoteDatasetAdmin;
import co.cask.cdap.remote.dataset.cube.CubeDatasetApp;
import co.cask.cdap.remote.dataset.cube.RemoteCube;
import co.cask.cdap.remote.dataset.kvtable.KVTableDatasetApp;
import co.cask.cdap.remote.dataset.kvtable.RemoteKeyValueTable;
import co.cask.cdap.remote.dataset.table.RemoteTable;
import co.cask.cdap.remote.dataset.table.TableDatasetApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Custom wrapper around IntegrationTestBase
 */
public class AudiTestBase extends IntegrationTestBase {
  protected static final Id.Namespace TEST_NAMESPACE = Id.Namespace.DEFAULT;
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);
  private final RESTClient restClient;

  public AudiTestBase() {
    restClient = createRestClient();
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

  protected List<RunRecord> getRunRecords(int expectedSize, final ProgramClient programClient, final Id.Program program,
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
    }, 30, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    return runRecords;
  }

  protected void assertRuns(int count, ProgramClient programClient,
                            ProgramRunStatus expectedStatus,  Id.Program... programIds) throws Exception {
    for (Id.Program programId : programIds) {
      List<RunRecord> runRecords =
        getRunRecords(count, programClient, programId, expectedStatus.name(), 0, Long.MAX_VALUE);
      Assert.assertEquals(count, runRecords.size());
      for (RunRecord runRecord : runRecords) {
        Assert.assertEquals(expectedStatus, runRecord.getStatus());
      }
    }
  }

  protected HttpResponse retryRestCalls(final int expectedStatusCode, final HttpRequest request,
                                        long timeout, TimeUnit timeoutUnit,
                                        long pollInterval, TimeUnit pollIntervalUnit) throws Exception {
    final AtomicReference<HttpResponse> ref = new AtomicReference<>();
    Tasks.waitFor(expectedStatusCode, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = getRestClient().execute(request, getClientConfig().getAccessToken());
        ref.set(response);
        return response.getResponseCode();
      }
    }, timeout, timeoutUnit, pollInterval, pollIntervalUnit);
    Preconditions.checkNotNull(ref.get(), "No Httprequest was attempted.");
    return ref.get();
  }

  // TODO: move the following four methods into IntegrationTestBase
  protected <T> DataSetManager<T> getDataset(String datasetName) throws Exception {
    return getTestManager().getDataset(TEST_NAMESPACE, datasetName);
  }

  protected ApplicationManager deployApplication(Id.Application appId, AppRequest appRequest) throws Exception {
    return getTestManager().deployApplication(appId, appRequest);
  }

  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz, Config configObject) {
    return getTestManager().deployApplication(TEST_NAMESPACE, applicationClz, configObject);
  }

  @SuppressWarnings("unchecked")
  protected final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                                String datasetTypeName, String datasetInstanceName,
                                                                DatasetProperties props) throws Exception {
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespace, datasetInstanceName);
    DatasetInstanceConfiguration dsConf = new DatasetInstanceConfiguration(datasetTypeName, props.getProperties());

    DatasetClient datasetClient = getDatasetClient();
    datasetClient.create(datasetInstance, dsConf);
    return (T) new RemoteDatasetAdmin(datasetClient, datasetInstance, dsConf);
  }

  // TODO: improve the following getXDataset methods. Eventually, move them into IntegrationTestBase.
  // Consider whether they should should have behavior of createIfNotExists.
  protected DataSetManager<Table> getTableDataset(String datasetName) throws Exception {
    return wrap(new RemoteTable(deployServiceForDataset(TableDatasetApp.class, datasetName),
                                getRestClient(), getClientConfig()));
  }

  protected DataSetManager<KeyValueTable> getKVTableDataset(String datasetName) throws Exception {
    return wrap(new RemoteKeyValueTable(deployServiceForDataset(KVTableDatasetApp.class, datasetName),
                                        getRestClient(), getClientConfig()));
  }

  protected DataSetManager<Cube> getCubeDataset(String datasetName) throws Exception {
    return wrap(new RemoteCube(deployServiceForDataset(CubeDatasetApp.class, datasetName),
                               getRestClient(), getClientConfig()));
  }

  // ensures that the Service for the dataset is deployed and running
  // returns its baseURL
  private URL deployServiceForDataset(Class<? extends Application> applicationClz,
                                      String datasetName) throws Exception {
    ApplicationManager appManager =
      deployApplication(applicationClz, new AbstractDatasetApp.DatasetConfig(datasetName));
    ServiceManager serviceManager =
      appManager.getServiceManager(AbstractDatasetApp.DatasetService.class.getSimpleName());

    URL serviceURL = serviceManager.getServiceURL();
    if (!serviceManager.isRunning()) {
      // start the service and wait until it becomes reachable. AbstractDatasetApp#DatasetService adds a PingHandler
      serviceManager.start();
      retryRestCalls(200, HttpRequest.get(new URL(serviceURL, "ping")).build(),
                     60, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
    }
    return serviceURL;
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
