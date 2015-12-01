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

package co.cask.cdap.remote.dataset.test;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.DataSetManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link co.cask.cdap.remote.dataset.cube.RemoteCube}.
 */
public class RemoteCubeTest extends AudiTestBase {

  @Test
  public void test() throws Exception {
    // create the Cube dataset
    DatasetProperties props = DatasetProperties.builder()
      .add("dataset.cube.resolutions", "1,60")
      .add("dataset.cube.aggregation.agg1.dimensions", "user,action")
      .add("dataset.cube.aggregation.agg1.requiredDimensions", "user,action").build();
    addDatasetInstance(TEST_NAMESPACE, Cube.class.getName(), "myCube", props);

    DataSetManager<Cube> cubeManager = getCubeDataset("myCube");
    Cube myCube = cubeManager.get();

    long tsInSec = System.currentTimeMillis() / 1000;

    // round to a minute for testing minute resolution
    tsInSec = (tsInSec / 60) * 60;

    // add couple facts
    myCube.add(ImmutableList.of(new CubeFact(tsInSec)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1)));

    myCube.add(ImmutableList.of(new CubeFact(tsInSec)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1),
                                new CubeFact(tsInSec + 1)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "back")
                                  .addMeasurement("count", MeasureType.COUNTER, 1),
                                new CubeFact(tsInSec + 2)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1)));

    // search for tags
    Collection<DimensionValue> tags =
      myCube.findDimensionValues(new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100,
                                                      new ArrayList<DimensionValue>()));
    Assert.assertEquals(1, tags.size());
    DimensionValue tv = tags.iterator().next();
    Assert.assertEquals("user", tv.getName());
    Assert.assertEquals("alex", tv.getValue());

    tags = myCube.findDimensionValues(CubeExploreQuery.builder()
                                        .from()
                                        .resolution(1, TimeUnit.SECONDS)
                                        .where()
                                        .dimension("user", "alex")
                                        .timeRange(tsInSec - 60, tsInSec + 60)
                                        .limit(100)
                                        .build());
    Assert.assertEquals(2, tags.size());
    Iterator<DimensionValue> iterator = tags.iterator();
    tv = iterator.next();
    Assert.assertEquals("action", tv.getName());
    Assert.assertEquals("back", tv.getValue());
    tv = iterator.next();
    Assert.assertEquals("action", tv.getName());
    Assert.assertEquals("click", tv.getValue());

    // search for measures
    Collection<String> measures =
      myCube.findMeasureNames(new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100,
                                                   ImmutableList.of(new DimensionValue("user", "alex"))));
    Assert.assertEquals(1, measures.size());
    String measure = measures.iterator().next();
    Assert.assertEquals("count", measure);

    // query for data

    // 1-sec resolution
    Collection<TimeSeries> data =
      myCube.query(CubeQuery.builder()
                     .select()
                     .measurement("count", AggregationFunction.SUM)
                     .from(null)
                     .resolution(1, TimeUnit.SECONDS)
                     .where()
                     .dimension("action", "click")
                     .timeRange(tsInSec - 60, tsInSec + 60)
                     .limit(100)
                     .build());
    Assert.assertEquals(1, data.size());
    TimeSeries series = data.iterator().next();
    List<TimeValue> timeValues = series.getTimeValues();
    Assert.assertEquals(2, timeValues.size());
    TimeValue timeValue = timeValues.get(0);
    Assert.assertEquals(tsInSec, timeValue.getTimestamp());
    Assert.assertEquals(2, timeValue.getValue());
    timeValue = timeValues.get(1);
    Assert.assertEquals(tsInSec + 2, timeValue.getTimestamp());
    Assert.assertEquals(1, timeValue.getValue());

    // 60-sec resolution
    data = myCube.query(new CubeQuery(null, tsInSec - 60, tsInSec + 60, 60, 100,
                                      ImmutableMap.of("count", AggregationFunction.SUM),
                                      ImmutableMap.of("action", "click"), new ArrayList<String>(), null));
    Assert.assertEquals(1, data.size());
    series = data.iterator().next();
    timeValues = series.getTimeValues();
    Assert.assertEquals(1, timeValues.size());
    timeValue = timeValues.get(0);
    Assert.assertEquals(tsInSec, timeValue.getTimestamp());
    Assert.assertEquals(3, timeValue.getValue());
  }
}
