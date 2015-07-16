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

package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom dataset example: counter table
 */
public class ExtendedCounterTableDefinition
  extends AbstractDatasetDefinition<ExtendedCounterTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public ExtendedCounterTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
    super(name);
    this.tableDef = tableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader cl) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("table"), cl);
  }

  @Override
  public ExtendedCounterTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                         Map<String, String> stringStringMap, ClassLoader cl) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("table"),
                                      new HashMap<String, String>(), cl);
    return new ExtendedCounterTableDataset(spec.getName(), table);
  }
}
