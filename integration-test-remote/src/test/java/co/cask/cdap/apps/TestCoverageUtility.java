package co.cask.cdap.apps;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceHandler;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetTypeHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.executor.ExploreExecutorHttpHandler;
import co.cask.cdap.explore.executor.ExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.ExploreStatusHandler;
import co.cask.cdap.explore.executor.NamespacedExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.NamespacedQueryExecutorHttpHandler;
import co.cask.cdap.explore.executor.QueryExecutorHttpHandler;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.router.RouterModules;
import co.cask.cdap.logging.gateway.handlers.LogHandler;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsHandler;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.http.ExceptionHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResourceModel;
import co.cask.http.HttpResponder;
import co.cask.http.PatternPathRouterWithGroups;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
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
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 *
 */
public class TestCoverageUtility {

  private final Logger LOG = LoggerFactory.getLogger(TestCoverageUtility.class);
  private final PatternPathRouterWithGroups<HttpResourceModel> patternRouter =
    PatternPathRouterWithGroups.create();
  private Set<String> handlerEndpointsCovered;
  private Set<String> handlerEndpoints;


  public Set<String> getAllHandlerEndpoints() {
    populateHandlerEndpoints();
    return handlerEndpoints;
  }

  private void populateHandlerEndpoints() {
    handlerEndpoints = Sets.newHashSet();
    handlerEndpointsCovered = Sets.newHashSet();
    final Injector injector = Guice.createInjector(createPersistentModules(CConfiguration.create(), new Configuration()));
    // app-fabric handlers
    Set<HttpHandler> handlers = (Set<HttpHandler>) injector.getInstance(Key.get(new TypeToken<Set<HttpHandler>>() {
    }.getType(), Names.named(Constants.AppFabric.HANDLERS_BINDING)));
    populateHandlerEndpoints(handlers);


    // explore handlers - since explore binding handlers set is in private module, we cannot access them directly.
    handlers = Sets.newHashSet();
    handlers.add(injector.getInstance(NamespacedQueryExecutorHttpHandler.class));
    handlers.add(injector.getInstance(QueryExecutorHttpHandler.class));
    handlers.add(injector.getInstance(NamespacedExploreMetadataHttpHandler.class));
    handlers.add(injector.getInstance(ExploreMetadataHttpHandler.class));
    handlers.add(injector.getInstance(ExploreExecutorHttpHandler.class));
    handlers.add(injector.getInstance(ExploreStatusHandler.class));
    // adding Metrics handler
    handlers.add(injector.getInstance(MetricsHandler.class));
    // logging handler
    handlers.add(injector.getInstance(LogHandler.class));

    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    DatasetDefinitionRegistry datasetDefinitionRegistry = new InMemoryDatasetDefinitionRegistry();

    DatasetDefinitionRegistryFactory registryFactory = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    };


    // dataset stuff are in private modules. creating these instances to get handler instance.
    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .build();

    TransactionExecutorFactory txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);


    RemoteDatasetFramework dsFramework = new RemoteDatasetFramework(new InMemoryDiscoveryService(),
                                                                    registryFactory);
    MDSDatasetsRegistry mdsDatasetsRegistry =
      new MDSDatasetsRegistry(new InMemoryTxSystemClient(
        new TransactionManager(injector.getInstance(Configuration.class))),
                              new InMemoryDatasetFramework(registryFactory, modules, cConf));

    ExploreFacade exploreFacade = new ExploreFacade(new DiscoveryExploreClient(new InMemoryDiscoveryService()), cConf);
    DatasetTypeManager typeManager = new DatasetTypeManager(cConf, mdsDatasetsRegistry, locationFactory,
                                                            Collections.<String, DatasetModule>emptyMap());
    DatasetInstanceService instanceService = new DatasetInstanceService(
      typeManager,
      new DatasetInstanceManager(mdsDatasetsRegistry),
      new InMemoryDatasetOpExecutor(dsFramework),
      exploreFacade,
      cConf,
      new UsageRegistry(txExecutorFactory, dsFramework));


    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, dsFramework, cConf);

    // adding type and instance handlers
    handlers.add(new DatasetInstanceHandler(instanceService));
    handlers.add(new DatasetTypeHandler(typeManager, cConf,
                                        new DefaultNamespacedLocationFactory(cConf, locationFactory)));

    // adding admin handler
    handlers.add(new DatasetAdminOpHTTPHandler(dsFramework, cConf, locationFactory, datasetInstantiatorFactory));
    populateHandlerEndpoints(handlers);
  }

  private static Set<HttpMethod> getHttpMethods(Method method) {
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

  public void printAllHandlerEndpoints() throws Exception {
    File allHandlers = new File("/tmp/all-endpoints.txt");
    try (PrintStream out = new PrintStream(new FileOutputStream(allHandlers))) {
      for (String endpoint : handlerEndpoints) {
        out.println(endpoint);
      }
    }
  }

  private String getInstanceURI() {
    return System.getProperty("instanceUri", "");
  }

  private String getAccessToken() {
    return System.getProperty("accessToken", "");
  }

  private ClientConfig getClientConfig() {
    ClientConfig.Builder builder = new ClientConfig.Builder();
    builder.setConnectionConfig(InstanceURIParser.DEFAULT.parse(
      URI.create(getInstanceURI()).toString()));

    if (!getAccessToken().isEmpty()) {
      builder.setAccessToken(new AccessToken(getAccessToken(), 0L, null));
    }

    builder.setDefaultConnectTimeout(120000);
    builder.setDefaultReadTimeout(120000);
    builder.setUploadConnectTimeout(0);
    builder.setUploadConnectTimeout(0);

    return builder.build();
  }

  public void printCoveredEndpoints() throws Exception {
    MetricsClient metricsClient = new MetricsClient(getClientConfig(), new RESTClient(getClientConfig()));
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

        }
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

}
