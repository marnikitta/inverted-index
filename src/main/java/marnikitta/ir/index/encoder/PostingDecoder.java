package marnikitta.ir.index.encoder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public final class PostingDecoder {

  public void decode(ByteBuffer from, LongBuffer to, long base) {
    long prevValue = base;

    while (from.remaining() > 0) {
      from.mark();
      final byte header = PostingDecoder.readHeader(from);
      final int bucketSize = PostingDecoder.high(header) + PostingDecoder.low(header);

      if (from.remaining() < bucketSize) {
        from.reset();
        return;
      }

      final long first = PostingDecoder.readNext(from, PostingDecoder.high(header)) + prevValue;
      prevValue = first;
      to.put(first);

      if (PostingDecoder.low(header) != 0) {
        final long second = PostingDecoder.readNext(from, PostingDecoder.low(header)) + prevValue;
        prevValue = second;
        to.put(second);
      }
    }
  }

  private static byte readHeader(ByteBuffer from) {
    return from.get();
  }

  private static int high(byte header) {
    return (header & 0xF0) >> 4;
  }

  private static int low(byte header) {
    return header & 0x0F;
  }

  private static long readNext(ByteBuffer from, int bytes) {
    long result = 0;
    for (int i = 0; i < bytes; ++i) {
      result |= Byte.toUnsignedLong(from.get()) << (i << 3);
    }
    return result;
  }
}
