package marnikitta.ir.index.indexer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class IndexChunk {
  public static final int DOCID_OFFSET = 31;

  private int size = 0;
  private final Map<CharSequence, long[]> postings = new HashMap<>();

  public void append(CharSequence word, int docId, int position) {
    final long encoding = this.encode(docId, position);
    this.postings.compute(word, (w, p) -> this.appendToPosting(p, encoding));

    this.size++;
  }

  private long[] appendToPosting(long[] posting, long newValue) {
    final long[] actualPosting;
    if (posting == null) {
      actualPosting = new long[2];
    } else if (posting[0] >= posting.length - 1) {
      actualPosting = Arrays.copyOf(posting, posting.length << 1);
    } else {
      actualPosting = posting;
    }

    actualPosting[(int) (actualPosting[0] + 1)] = newValue;
    actualPosting[0]++;

    return actualPosting;
  }

  public int size() {
    return this.size;
  }

  public Map<CharSequence, long[]> postings() {
    return this.postings;
  }

  private long encode(int docId, int position) {
    return ((long) docId << IndexChunk.DOCID_OFFSET) + position;
  }
}
