package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Module
 */
public class CounterTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
    CounterTableDefinition keyValueTable = new CounterTableDefinition("my_count_rowscannable_counterTable", table);
    registry.add(keyValueTable);
  }
}
