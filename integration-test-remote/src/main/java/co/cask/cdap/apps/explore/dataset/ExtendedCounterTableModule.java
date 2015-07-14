package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Module
 */
public class ExtendedCounterTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
    ExtendedCounterTableDefinition keyValueTable = new ExtendedCounterTableDefinition(
      "my_count_rowscannable_extendedCounterTable", table);
    registry.add(keyValueTable);
  }
}
