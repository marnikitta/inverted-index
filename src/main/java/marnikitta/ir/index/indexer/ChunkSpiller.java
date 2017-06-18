package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class ChunkSpiller {
  private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
  private final IndexChunk chunk;

  public ChunkSpiller(IndexChunk chunk) {
    this.chunk = chunk;
  }

  public void spill(FileChannel vocabChannel, FileChannel postingChannel) throws IOException {
    final Map<CharSequence, Integer> offsets = this.spillPostings(postingChannel);
    this.spillVocab(offsets, vocabChannel);
  }

  private void spillVocab(Map<CharSequence, Integer> offsets, FileChannel vocabChannel) {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);

    this.chunk.postings().forEach((c, p) -> {
      if (buffer.remaining() <= 1 + 4 + 128) {
        try {
          buffer.flip();
          vocabChannel.write(buffer);
          buffer.clear();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      final int lengthPlaceHolder = buffer.position();
      buffer.put((byte) 0);
      this.encoder.encode(CharBuffer.wrap(c), buffer, false);
      final byte length = (byte) (buffer.position() - lengthPlaceHolder - 1);

      buffer.put(lengthPlaceHolder, length);
      buffer.putInt(offsets.get(c));
    });
  }

  private Map<CharSequence, Integer> spillPostings(FileChannel postingChannel) throws IOException {
    final MappedByteBuffer buffer = postingChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.chunk.size() << 3);
    final LongBuffer lb = buffer.asLongBuffer();

    final Map<CharSequence, Integer> offsets = new HashMap<>(this.chunk.postings().size());

    final int[] offset = {0};
    this.chunk.postings().forEach((c, p) -> {
      offsets.put(c, offset[0]);
      lb.put(p, 1, (int) p[0]);
      offset[0] += p[0];
    });

    buffer.force();
    return offsets;
  }
}
