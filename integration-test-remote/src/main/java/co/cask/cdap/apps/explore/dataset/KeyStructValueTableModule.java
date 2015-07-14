package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;

/**
 * KeyStructValueTableModule
 */
public class KeyStructValueTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
    KeyStructValueTableDefinition keyValueTable = new KeyStructValueTableDefinition("structKeyValueTable", table);
    registry.add(keyValueTable);
  }
}