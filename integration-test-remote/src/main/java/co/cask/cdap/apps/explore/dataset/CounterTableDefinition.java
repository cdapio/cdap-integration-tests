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
public class CounterTableDefinition
  extends AbstractDatasetDefinition<CounterTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public CounterTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
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
    return tableDef.getAdmin(datasetContext, spec, cl);
  }

  @Override
  public CounterTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                 Map<String, String> stringStringMap, ClassLoader cl) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("table"),
                                      new HashMap<String, String>(), cl);
    return new CounterTableDataset(spec.getName(), table);
  }
}
