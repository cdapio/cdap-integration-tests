package co.cask.cdap.security;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.sportresults.ScoreCounter;
import co.cask.cdap.sportresults.SportResults;
import co.cask.cdap.sportresults.UploadService;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.wikipedia.SparkWikipediaClustering;
import co.cask.cdap.wikipedia.StreamToDataset;
import co.cask.cdap.wikipedia.TopNMapReduce;
import co.cask.cdap.wikipedia.WikiContentValidatorAndNormalizer;
import co.cask.cdap.wikipedia.WikipediaDataDownloader;
import co.cask.cdap.wikipedia.WikipediaPipelineApp;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test Wikipedia app with Impersonation across namespaces
 */
public class WikipediaImpersonationTest extends AudiTestBase {

  private static final NamespaceId NSDATASET1 = new NamespaceId("nsdataset1");
  private static final NamespaceId NSDATASET2 = new NamespaceId("nsdataset2");
  private static final NamespaceId NSAPP1 = new NamespaceId("nsapp1");
  private static final NamespaceId NSAPP2 = new NamespaceId("nsapp2");

  private static final String ALICE = "alice";
  private static final String EVE = "eve";
  private static final String BOB = "bob";

  private static final String GROUP_ADMIN = "admin";

  @Test
  public void testAcrossNamespaces() throws Exception {
    NamespaceClient namespaceClient = getNamespaceClient();

    // Create two namespaces for datasets
    registerForDeletion(NSDATASET1);
    NamespaceMeta ns1Meta = new NamespaceMeta.Builder()
      .setName(NSDATASET1)
      .setDescription("Namespace to hold the datasets")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_ADMIN)
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns1Meta);

    registerForDeletion(NSDATASET2);
    NamespaceMeta ns2Meta = new NamespaceMeta.Builder()
      .setName(NSDATASET2)
      .setDescription("Namespace to hold the datasets")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_ADMIN)
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns2Meta);

    // Deploy app as Alice in the above two namespaces
    ArtifactSummary artifactSummary =  new ArtifactSummary("WikipediaPipelineApp", "1.0.0");

    ArtifactId artifactId1 = NSDATASET1.artifact("WikipediaPipelineApp", "1.0.0");
    getTestManager().addAppArtifact(artifactId1, WikipediaPipelineApp.class);
    ApplicationId applicationId1 = NSDATASET1.app(WikipediaPipelineApp.class.getSimpleName());
    ApplicationManager applicationManager1 = deployApplication(applicationId1, new AppRequest(artifactSummary, null, ALICE));

    ArtifactId artifactId2 = NSDATASET2.artifact("WikipediaPipelineApp", "1.0.0");
    getTestManager().addAppArtifact(artifactId2, WikipediaPipelineApp.class);
    ApplicationId applicationId2 = NSDATASET2.app(WikipediaPipelineApp.class.getSimpleName());
    ApplicationManager applicationManager2 = deployApplication(applicationId2, new AppRequest(artifactSummary, null, ALICE));

    // Create a namespace to deploy the app as EVE
    registerForDeletion(NSAPP1);
    NamespaceMeta ns3Meta = new NamespaceMeta.Builder()
      .setName(NSAPP1)
      .setDescription("Namespace to hold the datasets")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_ADMIN)
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns3Meta);

    ArtifactId artifactId3 = NSAPP1.artifact("WikipediaPipelineApp", "1.0.0");
    getTestManager().addAppArtifact(artifactId3, WikipediaPipelineApp.class);
    ApplicationId applicationId3 = NSAPP1.app(WikipediaPipelineApp.class.getSimpleName());
    ApplicationManager applicationManager3 = deployApplication(applicationId3, new AppRequest(artifactSummary, null, EVE));

    setNamespaceArgs(applicationManager3, NSDATASET1);





    Map<String, String> args = new HashMap<>();
    // Have this program read and write from the Dataset in different namespace
    args.put("namespace", NS1.getNamespace());
    ServiceManager serviceManager =
      applicationManager.getServiceManager(UploadService.class.getSimpleName()).start(args);
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    serviceManager.waitForStatus(true);

    // upload a few dummy results
    URL url = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);
    uploadResults(url, "fantasy", 2014, FANTASY_2014);
    uploadResults(url, "fantasy", 2015, FANTASY_2015);
    uploadResults(url, "critters", 2014, CRITTERS_2014);

    args = new HashMap<>();
    args.put("league", "fantasy");
    // Have this program read and write from the Dataset in different namespace
    args.put("namespace", NS1.getNamespace());
    ApplicationManager applicationManager3 = applicationManager.getMapReduceManager(ScoreCounter.class.getSimpleName()).start(args);



    // Create a namespace to deploy the app as BOB
    registerForDeletion(NSAPP2);
    NamespaceMeta ns4Meta = new NamespaceMeta.Builder()
      .setName(NSAPP2)
      .setDescription("Namespace to hold the datasets")
      .setPrincipal(ALICE)
      .setGroupName(GROUP_ADMIN)
      .setKeytabURI(getKeytabURIforPrincipal(ALICE))
      .build();
    namespaceClient.create(ns3Meta);

    ArtifactId artifactId4 = NSAPP2.artifact("WikipediaPipelineApp", "1.0.0");
    getTestManager().addAppArtifact(artifactId4, WikipediaPipelineApp.class);
    ApplicationId applicationId4 = NSAPP2.app(WikipediaPipelineApp.class.getSimpleName());
    ApplicationManager applicationManager4 = deployApplication(applicationId4, new AppRequest(artifactSummary, null, BOB));
  }

  // write a file to the file set using the service
  private void uploadResults(URL url, String league, int season, String content) throws Exception {
    URL fullURL = new URL(url, String.format("leagues/%s/seasons/%d", league, season));
    HttpResponse response = getRestClient().execute(HttpMethod.PUT, fullURL, content,
                                                    ImmutableMap.<String, String>of("Content-type", "text/csv"),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  private String getKeytabURIforPrincipal(String principal) throws Exception {
    ConfigEntry configEntry = getMetaClient().getCDAPConfig().get(Constants.Security.KEYTAB_PATH);
    Preconditions.checkNotNull(configEntry, "Missing key from CDAP Configuration: %s", Constants.Security.KEYTAB_PATH);
    String name = new KerberosName(principal).getShortName();
    return configEntry.getValue().replace(Constants.USER_NAME_SPECIFIER, name);
  }

  private void createTestData(NamespaceId namespaceId) throws Exception {
    StreamId likesStream = namespaceId.stream("pageTitleStream");
    StreamManager likesStreamManager = getTestManager().getStreamManager(likesStream);
    String like1 = "{\"name\":\"Metallica\",\"id\":\"107926539230502\",\"created_time\":\"2015-06-25T17:14:47+0000\"}";
    String like2 = "{\"name\":\"grunge\",\"id\":\"911679552186992\",\"created_time\":\"2015-07-20T17:37:04+0000\"}";
    likesStreamManager.send(like1);
    likesStreamManager.send(like2);
  }

  private void setNamespaceArgs(ApplicationManager applicationManager, NamespaceId namespaceId) throws Exception {
    // Have this program read and write from the Dataset in different namespace
    Map<String, String> args = new HashMap<>();
    args.put("input_namespace", namespaceId.getNamespace());
    args.put("output_namespace", namespaceId.getNamespace());

    applicationManager.getMapReduceManager(StreamToDataset.class.getSimpleName()).setRuntimeArgs(args);
    applicationManager.getMapReduceManager(WikipediaDataDownloader.class.getSimpleName()).setRuntimeArgs(args);
    applicationManager.getMapReduceManager(WikiContentValidatorAndNormalizer.class.getSimpleName())
      .setRuntimeArgs(args);
    applicationManager.getMapReduceManager(TopNMapReduce.class.getSimpleName()).setRuntimeArgs(args);
    applicationManager.getSparkManager(SparkWikipediaClustering.class.getSimpleName()).setRuntimeArgs(args);
  }

  private void createTestData() throws Exception {
    StreamId likesStream = TEST_NAMESPACE.stream("pageTitleStream");
    StreamManager likesStreamManager = getTestManager().getStreamManager(likesStream);
    String like1 = "{\"name\":\"Metallica\",\"id\":\"107926539230502\",\"created_time\":\"2015-06-25T17:14:47+0000\"}";
    String like2 = "{\"name\":\"grunge\",\"id\":\"911679552186992\",\"created_time\":\"2015-07-20T17:37:04+0000\"}";
    likesStreamManager.send(like1);
    likesStreamManager.send(like2);
    StreamId rawWikiDataStream = TEST_NAMESPACE.stream("wikiStream");
    StreamManager rawWikipediaStreamManager = getTestManager().getStreamManager(rawWikiDataStream);
    String data1 = "{\"batchcomplete\":\"\",\"query\":{\"normalized\":[{\"from\":\"metallica\",\"to\":\"Metallica\"}]" +
      ",\"pages\":{\"18787\":{\"pageid\":18787,\"ns\":0,\"title\":\"Metallica\",\"revisions\":[{\"contentformat\":" +
      "\"text/x-wiki\",\"contentmodel\":\"wikitext\",\"*\":\"{{Other uses}}{{pp-semi|small=yes}}{{pp-move-indef|" +
      "small=yes}}{{Use mdy dates|date=April 2013}}{{Infobox musical artist|name = Metallica|image = Metallica at " +
      "The O2 Arena London 2008.jpg|caption = Metallica in [[London]] in 2008. From left to right: [[Kirk Hammett]], " +
      "[[Lars Ulrich]], [[James Hetfield]] and [[Robert Trujillo]]\"}]}}}}";
    String data2 = "{\"batchcomplete\":\"\",\"query\":{\"pages\":{\"51580\":{\"pageid\":51580,\"ns\":0," +
      "\"title\":\"Grunge\",\"revisions\":[{\"contentformat\":\"text/x-wiki\",\"contentmodel\":\"wikitext\"," +
      "\"*\":\"{{About|the music genre}}{{Infobox music genre| name  = Grunge| bgcolor = crimson| color = white| " +
      "stylistic_origins = {{nowrap|[[Alternative rock]], [[hardcore punk]],}} [[Heavy metal music|heavy metal]], " +
      "[[punk rock]], [[hard rock]], [[noise rock]]| cultural_origins  = Mid-1980s, [[Seattle|Seattle, Washington]], " +
      "[[United States]]| instruments = [[Electric guitar]], [[bass guitar]], [[Drum kit|drums]], " +
      "[[Singing|vocals]]| derivatives = [[Post-grunge]], [[nu metal]]| subgenrelist = | subgenres = | fusiongenres" +
      "      = | regional_scenes   = [[Music of Washington (state)|Washington state]]| other_topics      = * " +
      "[[Alternative metal]]* [[Generation X]]* [[Grunge speak|grunge speak hoax]]* [[timeline of alternative " +
      "rock]]}}'''Grunge''' (sometimes referred to as the '''Seattle sound''') is a subgenre of [[alternative rock]]" +
      " that emerged during the mid-1980s in the American state of [[Washington (state)|Washington]], particularly " +
      "in [[Seattle]].  The early grunge movement revolved around Seattle's [[independent record label]] " +
      "[[Sub Pop]], but by the early 1990s its popularity had spread, with grunge acts in California and other " +
      "parts of the U.S. building strong followings and signing major record deals.Grunge became commercially " +
      "successful in the first half of the 1990s, due mainly to the release of [[Nirvana (band)|Nirvana]]'s " +
      "''[[Nevermind]]'', [[Pearl Jam]]'s ''[[Ten (Pearl Jam album)|Ten]]'', [[Soundgarden]]'s " +
      "''[[Badmotorfinger]]'', [[Alice in Chains]]' ''[[Dirt (Alice in Chains album)|Dirt]]'', and " +
      "[[Stone Temple Pilots]]' ''[[Core (Stone Temple Pilots album)|Core]]''.\"}]}}}}";
    rawWikipediaStreamManager.send(data1);
    rawWikipediaStreamManager.send(data2);

    waitForStreamToBePopulated(likesStreamManager, 2);
    waitForStreamToBePopulated(rawWikipediaStreamManager, 2);
  }

  private void waitForStreamToBePopulated(final StreamManager streamManager, int numEvents) throws Exception {
    Tasks.waitFor(numEvents, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        List<StreamEvent> streamEvents = streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE);
        return streamEvents.size();
      }
    }, 10, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }
}
