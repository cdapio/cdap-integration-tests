package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * KeyStructValueTable
 */
public class KeyStructValueTable extends AbstractDataset implements
  RecordScannable<KeyStructValueTableDefinition.KeyValue>,
  RecordWritable<KeyStructValueTableDefinition.KeyValue> {

  static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};
  private static final Gson GSON = new Gson();

  private final Table table;

  public KeyStructValueTable(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  public void put(String key, KeyStructValueTableDefinition.KeyValue.Value value) throws Exception {
    table.put(Bytes.toBytes(key), COL, Bytes.toBytes(GSON.toJson(value)));
  }

  public KeyStructValueTableDefinition.KeyValue.Value get(String key) throws Exception {
    return GSON.fromJson(Bytes.toString(table.get(Bytes.toBytes(key), COL)),
                         KeyStructValueTableDefinition.KeyValue.Value.class);
  }

  @Override
  public void write(KeyStructValueTableDefinition.KeyValue keyValue) throws IOException {
    try {
      put(keyValue.getKey(), keyValue.getValue());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Type getRecordType() {
    return KeyStructValueTableDefinition.KeyValue.class;
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<KeyStructValueTableDefinition.KeyValue> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(table.createSplitReader(split), KEY_VALUE_ROW_MAKER);
  }

  private  final Scannables.RecordMaker<byte[], Row, KeyStructValueTableDefinition.KeyValue> KEY_VALUE_ROW_MAKER =
    new Scannables.RecordMaker<byte[], Row, KeyStructValueTableDefinition.KeyValue>() {
      @Override
      public KeyStructValueTableDefinition.KeyValue makeRecord(byte[] key, Row row) {
        KeyStructValueTableDefinition.KeyValue keyValue = new
          KeyStructValueTableDefinition.KeyValue(Bytes.toString(key), GSON.fromJson
          (Bytes.toString(row.get(KeyStructValueTable.COL)), KeyStructValueTableDefinition.KeyValue.Value.class));
        return keyValue;
      }
    };
}
