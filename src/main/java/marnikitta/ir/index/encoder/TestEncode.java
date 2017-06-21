package marnikitta.ir.index.encoder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Random;

public final class TestEncode {
  public static void main(final String... args) {
    final long[] longs = new Random().longs().limit(11237).toArray();
    final LongBuffer longBuffer = LongBuffer.wrap(longs);
    final ByteBuffer byteBuffer = ByteBuffer.allocate(longs.length << 4);

    new PostingEncoder().encode(longBuffer, byteBuffer, true);

    final LongBuffer anotherBuffer = LongBuffer.allocate(longs.length);

    byteBuffer.flip();

    final int lim = byteBuffer.limit() / 2;
    final int prevLimit = byteBuffer.limit();
    byteBuffer.limit(lim);
    new PostingDecoder().decode(byteBuffer, anotherBuffer, 0);

    byteBuffer.limit(prevLimit);
    new PostingDecoder().decode(byteBuffer, anotherBuffer, anotherBuffer.get(anotherBuffer.position() - 1));

    longBuffer.flip();
    anotherBuffer.flip();
    System.out.println(longBuffer.equals(anotherBuffer));
  }
}
