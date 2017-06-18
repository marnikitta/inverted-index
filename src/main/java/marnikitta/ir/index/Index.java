package marnikitta.ir.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;

public final class Index {
  public static final int DOCID_OFFSET = 32;

  private int size = 0;
  private final HashMap<CharSequence, Integer> vocabulary;

  private int wordCount = 0;
  private long[][] postings;
  private int[] postingSizes;

  public Index() {
    this.vocabulary = new HashMap<>();
    this.postings = new long[1][];
    this.postingSizes = new int[1];
  }

  public void append(CharSequence word, int docId, int position) {
    final int wordId = this.vocabulary.computeIfAbsent(word, w -> this.vocabulary.size());
    final long encoding = this.encode(docId, position);

    if (wordId >= this.wordCount) {
      this.addNewPosting();
    }

    this.appendToPosting(wordId, encoding);
    this.size++;
  }

  private void addNewPosting() {
    if (this.wordCount >= this.postings.length) {
      this.postings = Arrays.copyOf(this.postings, this.postings.length << 1);
      this.postingSizes = Arrays.copyOf(this.postingSizes, this.postingSizes.length << 1);
    }

    this.postings[this.wordCount] = new long[1];
    this.postingSizes[this.wordCount] = 0;
    this.wordCount++;
  }

  private void appendToPosting(int wordId, long newValue) {
    if (this.postingSizes[wordId] >= this.postings[wordId].length) {
      this.postings[wordId] = Arrays.copyOf(this.postings[wordId], this.postings[wordId].length << 1);
    }

    this.postings[wordId][this.postingSizes[wordId]] = newValue;
    this.postingSizes[wordId]++;
  }

  public int size() {
    return this.size;
  }

  public void spill(FileChannel vocabChannel, FileChannel postingChannel) throws IOException {
    final CharSequence[] vocab = new String[this.vocabulary.size()];
    this.vocabulary.forEach((word, wordId) -> vocab[wordId] = word);

    final ByteBuffer vocabBuffer = ByteBuffer.allocate(8192);

    final int[] offsets = this.spillPostings(postingChannel);
  }

  private int[] spillPostings(FileChannel postingChannel) throws IOException {
    final MappedByteBuffer buffer = postingChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.size << 3);
    final LongBuffer lb = buffer.asLongBuffer();

    final int[] offsets = new int[this.vocabulary.size()];

    int offset = 0;
    for (int i = 0; i < this.wordCount; ++i) {
      offsets[i] = offset;
      Arrays.sort(this.postings[i]);
      lb.put(this.postings[i]);
      offset += this.postingSizes[i];
    }

    buffer.force();
    return offsets;
  }

  private long encode(int docId, int position) {
    return (long) docId << Index.DOCID_OFFSET + position;
  }
}
