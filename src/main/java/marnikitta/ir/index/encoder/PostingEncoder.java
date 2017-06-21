package marnikitta.ir.index.encoder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public final class PostingEncoder {

  public void encode(LongBuffer from, ByteBuffer to, boolean spillLast) {
    final int postingSize = from.remaining();
    final int evenLength = postingSize >> 1 << 1;

    long prevEntry = 0;

    for (int i = 0; i < evenLength; i += 2) {
      final long first = from.get();
      final long second = from.get();
      this.spillPair(to, first - prevEntry, second - first);
      prevEntry = second;
    }

    if (spillLast && from.remaining() != 0) {
      this.spillLast(to, from.get() - prevEntry);
    }
  }

  private void spillPair(ByteBuffer buffer, long a, long b) {
    final int headerPosition = buffer.position();
    buffer.put((byte) 0);

    final byte firstSize = this.writeVarLong(buffer, a);
    final byte secondSize = this.writeVarLong(buffer, b);

    final byte header = (byte) (firstSize << 4 | secondSize);
    buffer.put(headerPosition, header);
  }

  private void spillLast(ByteBuffer buffer, long last) {
    final int headerPosition = buffer.position();
    buffer.put((byte) 0);

    final byte header = (byte) (this.writeVarLong(buffer, last) << 4);
    buffer.put(headerPosition, header);
  }

  private byte writeVarLong(ByteBuffer buffer, long a) {
    byte lenght = 0;
    long currLong = a;
    for (lenght = 0; currLong != 0; ++lenght) {
      final byte b = (byte) (currLong & 0xFF);
      buffer.put(b);
      currLong = (currLong & ~0xFF) >>> 8;
    }

    return lenght;
  }
}
