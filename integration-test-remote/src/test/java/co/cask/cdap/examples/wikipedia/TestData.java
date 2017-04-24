/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestManager;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to create and send test data by ITN using {@link WikipediaPipelineApp} Example
 */
public class TestData {
  public static void sendTestData(NamespaceId namespaceId, TestManager testManager) throws Exception {
    StreamId likesStream = namespaceId.stream("pageTitleStream");
    StreamManager likesStreamManager = testManager.getStreamManager(likesStream);
    String like1 = "{\"name\":\"Metallica\",\"id\":\"107926539230502\",\"created_time\":\"2015-06-25T17:14:47+0000\"}";
    String like2 = "{\"name\":\"grunge\",\"id\":\"911679552186992\",\"created_time\":\"2015-07-20T17:37:04+0000\"}";
    likesStreamManager.send(like1);
    likesStreamManager.send(like2);
    StreamId rawWikiDataStream = namespaceId.stream("wikiStream");
    StreamManager rawWikipediaStreamManager = testManager.getStreamManager(rawWikiDataStream);
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

  private static void waitForStreamToBePopulated(final StreamManager streamManager, int numEvents) throws Exception {
    Tasks.waitFor(numEvents, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        List<StreamEvent> streamEvents = streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE);
        return streamEvents.size();
      }
    }, 10, TimeUnit.SECONDS, 500, TimeUnit.MILLISECONDS);
  }
}
