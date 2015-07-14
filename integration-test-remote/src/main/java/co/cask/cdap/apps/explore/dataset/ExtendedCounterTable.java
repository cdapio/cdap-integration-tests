package co.cask.cdap.apps.explore.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;

/**
 *
 */
public interface ExtendedCounterTable extends Dataset, BatchReadable<String, Long>, BatchWritable<String, Long>,
  RecordScannable<ExtendedCounterTable.Count>, RecordWritable<ExtendedCounterTable.Count> {
  void inc(String key, long value);
  long get(String key);

  public class Count {
    private final String word;
    private final long count;
    private final long timestamp;

    public Count(String word, Long count, Long timestamp) {
      this.word = word;
      this.count = count;
      this.timestamp = timestamp;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getWord() {
      return word;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getCount() {
      return count;
    }

    @SuppressWarnings("UnusedDeclaration")
    public long getTimestamp() {
      return timestamp;
    }
  }
}
