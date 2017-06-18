package marnikitta.ir.index.indexer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class SpilledPosting implements Closeable {
  private final FileChannel channel;

  public SpilledPosting(Path path) throws IOException {
    this.channel = FileChannel.open(path);
  }

  @Override
  public void close() throws IOException {
    this.channel.close();
  }

  public static Vocabulary spillPostings(IndexChunk chunk, FileChannel channel) throws IOException {
    final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long)chunk.size() << 3);
    final LongBuffer lb = buffer.asLongBuffer();

    final Map<CharSequence, PostingLocation> offsets = new HashMap<>(chunk.postings().size());

    final long[] offset = {0};
    chunk.postings().forEach((c, p) -> {
      offsets.put(c, new PostingLocation(offset[0], p[0] << 3));
      lb.put(p, 1, (int) p[0]);
      offset[0] += p[0];
    });

    buffer.force();
    return new Vocabulary(offsets);
  }
}
