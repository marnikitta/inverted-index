package marnikitta.ir.index.indexer;

import marnikitta.ir.index.encoder.PostingEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SpilledPosting {
  private static final byte DOCID_OFFSET = (byte) 32;
  private static final long DOCID_MASK = ~((1L << 32) - 1);
  private static final PostingEncoder ENCODER = new PostingEncoder();

  private final FileChannel postingChannel;

  public SpilledPosting(FileChannel channel) {
    this.postingChannel = channel;
  }

  public Set<Integer> intersect(Collection<PostingLocation> locations, int limit) throws IOException {
    final List<PostingIterator> iterators = locations.stream().distinct()
            .map(l -> new PostingIterator(l, this.postingChannel)).collect(Collectors.toList());

    return SpilledPosting.doIntersect(iterators, limit);
  }

  private static Set<Integer> doIntersect(Iterable<PostingIterator> iterators, int limit) throws IOException {
    final Set<Integer> documents = new HashSet<>();

    int currentDoc = 0;
    boolean iteratorsHaveNext = true;

    while (documents.size() < limit && iteratorsHaveNext) {
      boolean currentDocumentVerdict = true;

      for (PostingIterator it : iterators) {
        it.seekLower(SpilledPosting.encode(currentDoc, 0));

        if (!it.hasNext()) {
          iteratorsHaveNext = false;
          currentDocumentVerdict = false;
          break;
        } else if (currentDoc != SpilledPosting.docIdOf(it.peek())) {
          currentDocumentVerdict = false;
          break;
        }
      }

      if (currentDocumentVerdict) {
        documents.add(currentDoc);
      }
      currentDoc += 1;
    }

    return documents;
  }

  public static Vocabulary spill(IndexChunk chunk, FileChannel channel) throws IOException {
    final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) chunk.size() << 3);

    final Map<CharSequence, PostingLocation> vocab = new HashMap<>(chunk.postings().size());

    final long[] offset = {0};
    chunk.postings().forEach((c, p) -> {
      final int written = SpilledPosting.spillPosting(p, buffer);
      vocab.put(c, new PostingLocation(offset[0], written));
      offset[0] += written;
    });

    buffer.force();
    return new Vocabulary(vocab);
  }

  private static int spillPosting(long[] posting, ByteBuffer buffer) {
    final int pos = buffer.position();
    SpilledPosting.ENCODER.encode(LongBuffer.wrap(posting, 1, (int) posting[0]), buffer, true);
    return buffer.position() - pos;
  }

  public static int docIdOf(long encoding) {
    return (int) ((encoding & SpilledPosting.DOCID_MASK) >> SpilledPosting.DOCID_OFFSET);
  }

  public static long encode(int docId, int position) {
    return ((long) docId << SpilledPosting.DOCID_OFFSET) + position;
  }
}
