package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;

/**
 *
 */
public interface CounterTable extends Dataset, BatchReadable<String, Long>, BatchWritable<String, Long>,
  RecordScannable<CounterTable.Count>, RecordWritable<CounterTable.Count> {
  void inc(String key, long value);
  long get(String key);

  public class Count {
    private final String word;
    private final long count;

    public Count(String word, Long count) {
      this.word = word;
      this.count = count;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getWord() {
      return word;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getCount() {
      return count;
    }
  }
}
