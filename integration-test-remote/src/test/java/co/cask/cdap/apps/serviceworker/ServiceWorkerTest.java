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

package co.cask.cdap.apps.serviceworker;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.apps.AudiTestBase;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.explore.executor.ExploreExecutorHttpHandler;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.executor.ExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.ExploreStatusHandler;
import co.cask.cdap.explore.executor.NamespacedExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.NamespacedQueryExecutorHttpHandler;
import co.cask.cdap.explore.executor.QueryExecutorHttpHandler;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.router.RouterModules;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.http.ExceptionHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResourceModel;
import co.cask.http.HttpResponder;
import co.cask.http.PatternPathRouterWithGroups;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Test worker that writes to dataset and service that reads from it.
 */
public class ServiceWorkerTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AudiTestBase.class);

  private Set<String> handlerEndpointsCovered;
  private Set<String> handlerEndpoints;
  private Set<String> badRestCalls;
  private Set<String> notFoundEndpoints;
  private final PatternPathRouterWithGroups<HttpResourceModel> patternRouter = PatternPathRouterWithGroups.create();

  @Before
  public void before() throws Exception {
    handlerEndpoints = Sets.newHashSet();
    handlerEndpointsCovered = Sets.newHashSet();
    badRestCalls = Sets.newHashSet();
    notFoundEndpoints = Sets.newHashSet();
    Injector injector = Guice.createInjector(createPersistentModules(CConfiguration.create(), new Configuration()));
    Set<HttpHandler> handlers = (Set<HttpHandler>) injector.getInstance(Key.get(new TypeToken<Set<HttpHandler>>() {
    }.getType(), Names.named(Constants.AppFabric.HANDLERS_BINDING)));
    populateHandlerEndpoints(handlers);

    handlers = Sets.newHashSet();
    handlers.add(injector.getInstance(NamespacedQueryExecutorHttpHandler.class));
    handlers.add(injector.getInstance(QueryExecutorHttpHandler.class));
    handlers.add(injector.getInstance(NamespacedExploreMetadataHttpHandler.class));
    handlers.add(injector.getInstance(ExploreMetadataHttpHandler.class));
    handlers.add(injector.getInstance(ExploreExecutorHttpHandler.class));
    handlers.add(injector.getInstance(ExploreStatusHandler.class));

    populateHandlerEndpoints(handlers);
  }

  private Set<HttpMethod> getHttpMethods(Method method) {
    Set<HttpMethod> httpMethods = Sets.newHashSet();

    if (method.isAnnotationPresent(GET.class)) {
      httpMethods.add(HttpMethod.GET);
    }
    if (method.isAnnotationPresent(PUT.class)) {
      httpMethods.add(HttpMethod.PUT);
    }
    if (method.isAnnotationPresent(POST.class)) {
      httpMethods.add(HttpMethod.POST);
    }
    if (method.isAnnotationPresent(DELETE.class)) {
      httpMethods.add(HttpMethod.DELETE);
    }

    return ImmutableSet.copyOf(httpMethods);
  }


  private void populateHandlerEndpoints(Set<HttpHandler> handlers) {
    for (HttpHandler handler : handlers) {
      String basePath = "";
      if (handler.getClass().isAnnotationPresent(Path.class)) {
        basePath = handler.getClass().getAnnotation(Path.class).value();
      }

      for (Method method : handler.getClass().getDeclaredMethods()) {
        if (method.getParameterTypes().length >= 2 &&
          method.getParameterTypes()[0].isAssignableFrom(org.jboss.netty.handler.codec.http.HttpRequest.class) &&
          method.getParameterTypes()[1].isAssignableFrom(HttpResponder.class) &&
          Modifier.isPublic(method.getModifiers())) {

          String relativePath = "";
          if (method.getAnnotation(Path.class) != null) {
            relativePath = method.getAnnotation(Path.class).value();
          }
          String absolutePath = String.format("%s/%s", basePath, relativePath);
          Set<HttpMethod> httpMethods = getHttpMethods(method);
          Preconditions.checkArgument(httpMethods.size() >= 1,
                                      String.format("No HttpMethod found for method: %s", method.getName()));
          patternRouter.add(absolutePath, new HttpResourceModel(httpMethods, absolutePath, method,
                                                                handler, new ExceptionHandler()));
          for (HttpMethod httpMethod : httpMethods) {
            handlerEndpoints.add(httpMethod.getName() + ":" + absolutePath);
          }

        } else {
          LOG.trace("Not adding method {}({}) to path routing like. HTTP calls will not be routed to this method",
                    method.getName(), method.getParameterTypes());
        }
      }
    }
  }


  private static List<Module> createPersistentModules(CConfiguration configuration, Configuration hConf) {
    configuration.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, Constants.DEFAULT_DATA_LEVELDB_DIR);

    String environment =
      configuration.get(Constants.CFG_APPFABRIC_ENVIRONMENT, Constants.DEFAULT_APPFABRIC_ENVIRONMENT);
    if (environment.equals("vpc")) {
      System.err.println("Application Server Environment: " + environment);
    }

    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new MetricsHandlerModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new LocationRuntimeModule().getStandaloneModules(),
      new AppFabricServiceRuntimeModule().getStandaloneModules(),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new RouterModules().getStandaloneModules(),
      new SecurityModules().getStandaloneModules(),
      new StreamServiceRuntimeModule().getStandaloneModules(),
      new ExploreRuntimeModule().getStandaloneModules(),
      new ServiceStoreModules().getStandaloneModules(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getStandaloneModules(),
      new NotificationServiceRuntimeModule().getStandaloneModules(),
      new StreamAdminModules().getStandaloneModules()
    );
  }

  @After
  public void postExecution() throws Exception {
    MetricsClient metricsClient = getMetricsClient();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                               Constants.Metrics.Tag.COMPONENT, "*",
                                               Constants.Metrics.Tag.HANDLER, "*");

    List<MetricTagValue> tagValues = metricsClient.searchTags(tags);
    for (MetricTagValue tagValue : tagValues) {
      if (tagValue.getName().equals("method") && tagValue.getValue() != null) {
        String[] request = tagValue.getValue().split(":");
        if (request.length != 2) {
          continue;
        }
        String method = request[0];
        String path = request[1];
        List<PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel>> routableDestinations
          = patternRouter.getDestinations(path);

        PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel> matchedDestination =
          getMatchedDestination(routableDestinations, HttpMethod.valueOf(method), path);

        if (matchedDestination != null) {
          //Found a httpresource route to it.
          HttpResourceModel httpResourceModel = matchedDestination.getDestination();
          handlerEndpointsCovered.add(method + ":" + httpResourceModel.getPath());

        } else if (routableDestinations.size() > 0)  {
          //Found a matching resource but could not find the right HttpMethod so return 405
          badRestCalls.add(path);
        } else {

        }
      }
    }
    File allHandlers = new File("/tmp/all-endpoints.txt");
    try (PrintStream out = new PrintStream(new FileOutputStream(allHandlers))) {
      for (String endpoint : handlerEndpoints) {
        out.println(endpoint);
      }
    }

    File coveredEndpoints = new File("/tmp/covered-endpoints.txt");
    try (PrintStream out = new PrintStream(new FileOutputStream(coveredEndpoints))) {
      for (String endpoint : handlerEndpointsCovered) {
        out.println(endpoint);
      }
    }
  }


  /**
   * Get HttpResourceModel which matches the HttpMethod of the request.
   *
   * @param routableDestinations List of ResourceModels.
   * @param targetHttpMethod HttpMethod.
   * @param requestUri request URI.
   * @return RoutableDestination that matches httpMethod that needs to be handled. null if there are no matches.
   */
  private PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel>
  getMatchedDestination(List<PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel>> routableDestinations,
                        HttpMethod targetHttpMethod, String requestUri) {

    Iterable<String> requestUriParts = Splitter.on('/').omitEmptyStrings().split(requestUri);
    List<PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel>> matchedDestinations =
      Lists.newArrayListWithExpectedSize(routableDestinations.size());
    int maxExactMatch = 0;
    int maxGroupMatch = 0;
    int maxPatternLength = 0;

    for (PatternPathRouterWithGroups.RoutableDestination<HttpResourceModel> destination : routableDestinations) {
      HttpResourceModel resourceModel = destination.getDestination();
      int groupMatch = destination.getGroupNameValues().size();

      for (HttpMethod httpMethod : resourceModel.getHttpMethod()) {
        if (targetHttpMethod.equals(httpMethod)) {

          int exactMatch = getExactPrefixMatchCount(
            requestUriParts, Splitter.on('/').omitEmptyStrings().split(resourceModel.getPath()));

          // When there are multiple matches present, the following precedence order is used -
          // 1. template path that has highest exact prefix match with the url is chosen.
          // 2. template path has the maximum groups is chosen.
          // 3. finally, template path that has the longest length is chosen.
          if (exactMatch > maxExactMatch) {
            maxExactMatch = exactMatch;
            maxGroupMatch = groupMatch;
            maxPatternLength = resourceModel.getPath().length();

            matchedDestinations.clear();
            matchedDestinations.add(destination);
          } else if (exactMatch == maxExactMatch && groupMatch >= maxGroupMatch) {
            if (groupMatch > maxGroupMatch || resourceModel.getPath().length() > maxPatternLength) {
              maxGroupMatch = groupMatch;
              maxPatternLength = resourceModel.getPath().length();
              matchedDestinations.clear();
            }
            matchedDestinations.add(destination);
          }
        }
      }
    }

    if (matchedDestinations.size() > 1) {
      throw new IllegalStateException(String.format("Multiple matched handlers found for request uri %s", requestUri));
    } else if (matchedDestinations.size() == 1) {
      return matchedDestinations.get(0);
    }
    return null;
  }

  private int getExactPrefixMatchCount(Iterable<String> first, Iterable<String> second) {
    int count = 0;
    for (Iterator<String> fit = first.iterator(), sit = second.iterator(); fit.hasNext() && sit.hasNext(); ) {
      if (fit.next().equals(sit.next())) {
        ++count;
      } else {
        break;
      }
    }
    return count;
  }

  @Test
  public void test() throws Exception {
    RESTClient restClient = getRestClient();
    ApplicationManager applicationManager = deployApplication(ServiceApplication.class);

    ServiceManager serviceManager = applicationManager.getServiceManager(ServiceApplication.SERVICE_NAME).start();
    serviceManager.waitForStatus(true, 60, 1);


    // TODO: better way to wait for service to be up.
    TimeUnit.SECONDS.sleep(60);
    URL serviceURL = serviceManager.getServiceURL();
    URL url = new URL(serviceURL, "read/" + DatasetWorker.WORKER_DATASET_TEST_KEY);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());

    // hit the service endpoint, get for worker_key, should return 204 (null)
    Assert.assertEquals(204, response.getResponseCode());

    // start the worker
    WorkerManager workerManager = applicationManager.getWorkerManager(ServiceApplication.WORKER_NAME).start();
    workerManager.waitForStatus(true, 60, 1);
    TimeUnit.SECONDS.sleep(10);

    // check if the worker's write to the table was successful
    waitForWorkerResult(url, restClient, 1, 60, TimeUnit.SECONDS);

    // todo : this shouldn't be necessary as the worker exits after writing to table
    TimeUnit.SECONDS.sleep(10);
    workerManager.stop();
    workerManager.waitForStatus(false, 60, 1);

    // try starting the service , while its running, should throw IllegalArgumentException
    boolean alreadyRunning = false;
    try {
      serviceManager.start();
    } catch (Exception e) {
      alreadyRunning = (e instanceof IllegalStateException);
    }
    Assert.assertTrue(alreadyRunning);

    serviceManager.stop();
    serviceManager.waitForStatus(false, 60, 1);
  }

  protected void waitForWorkerResult(URL url, RESTClient restClient, long sleepTime, long timeout,
                                     TimeUnit timeoutUnit) throws Exception {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    HttpResponse response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
    long startTime = System.currentTimeMillis();
    boolean statusMatched = (response.getResponseCode() == 200);
    while (!statusMatched && (System.currentTimeMillis() - startTime) < timeoutMillis) {
      timeoutUnit.sleep(sleepTime);
      response = restClient.execute(HttpRequest.get(url).build(), getClientConfig().getAccessToken());
      statusMatched = (response.getResponseCode() == 200);
    }

    if (!statusMatched) {
      throw new IllegalStateException("Program state not as expected. Expected " + 200);
    }
    // check value returned
    Assert.assertEquals("\"" + DatasetWorker.WORKER_DATASET_TEST_VALUE + "\"",
                        Bytes.toString(response.getResponseBody()));
  }

}
