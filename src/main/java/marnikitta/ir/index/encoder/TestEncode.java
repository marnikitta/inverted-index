package marnikitta.ir.index.encoder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Random;

public final class TestEncode {
  public static void main(final String... args) {
    long[] longs = new Random().longs().limit(1127).toArray();
    LongBuffer longBuffer = LongBuffer.wrap(longs);
    ByteBuffer byteBuffer = ByteBuffer.allocate(longs.length << 4);

    LongBuffer anotherBuffer = LongBuffer.allocate(longs.length);

    new PostingEncoder().encode(longBuffer, byteBuffer, true);

    byteBuffer.flip();

    new PostingDecoder().decode(byteBuffer, anotherBuffer, 0);

    System.out.println(longBuffer.equals(anotherBuffer));
  }
}
