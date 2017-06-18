package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;

public final class Vocabulary {
  private static final int MAX_ENTRY_SIZE = 1 + 8 + 8 + 128;

  private final Map<CharSequence, PostingLocation> vocabulary;

  public Vocabulary(Map<CharSequence, PostingLocation> vocabulary) {
    this.vocabulary = vocabulary;
  }

  public Map<CharSequence, PostingLocation> payload() {
    return this.vocabulary;
  };

  public void spill(FileChannel channel, CharsetEncoder encoder) {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);

    this.vocabulary.forEach((c, p) -> {
      if (buffer.remaining() <= Vocabulary.MAX_ENTRY_SIZE) {
        try {
          buffer.flip();
          channel.write(buffer);
          buffer.clear();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      final int lengthPlaceHolder = buffer.position();
      buffer.put((byte) 0);
      encoder.encode(CharBuffer.wrap(c), buffer, false);
      final byte length = (byte) (buffer.position() - lengthPlaceHolder - 1);

      buffer.put(lengthPlaceHolder, length);
      buffer.putLong(this.vocabulary.get(c).offset);
      buffer.putLong(this.vocabulary.get(c).length);
    });
  }

  public static Vocabulary load(FileChannel channel, CharsetDecoder decoder) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);
    final ByteBuffer slice = buffer.asReadOnlyBuffer();

    final Map<CharSequence, PostingLocation> result = new HashMap<>();

    while (channel.read(buffer) > 0) {
      buffer.flip();
      while (buffer.remaining() > Vocabulary.MAX_ENTRY_SIZE) {
        Vocabulary.readInto(result, buffer, slice, decoder);
      }
      buffer.compact();
    }

    buffer.flip();

    while (buffer.remaining() > 0) {
      Vocabulary.readInto(result, buffer, slice, decoder);
    }

    return new Vocabulary(result);
  }

  private static void readInto(Map<CharSequence, PostingLocation> destination,
                               ByteBuffer buffer,
                               ByteBuffer slice,
                               CharsetDecoder decoder) throws CharacterCodingException {
    final byte wordLength = buffer.get();

    slice.limit(buffer.position() + wordLength).position(buffer.position());
    final CharBuffer word = decoder.decode(slice);

    buffer.position(buffer.position() + wordLength);
    final long offset = buffer.getLong();
    final long length = buffer.getLong();

    destination.put(word, new PostingLocation(offset, length));
  }
}
