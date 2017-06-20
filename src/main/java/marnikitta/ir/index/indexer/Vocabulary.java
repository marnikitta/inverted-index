package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class Vocabulary {
  private static final int MAX_ENTRY_SIZE = 1 + 8 + 8 + 128;

  private final Map<CharSequence, PostingLocation> vocabulary;

  public Vocabulary(Map<CharSequence, PostingLocation> vocabulary) {
    this.vocabulary = vocabulary;
  }

  public Optional<PostingLocation> get(CharSequence q) {
    return Optional.ofNullable(this.vocabulary.get(q));
  }

  public void spill(FileChannel channel, CharsetEncoder encoder) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

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

      Vocabulary.putEntry(buffer, c, p, encoder);
    });

    buffer.flip();
    channel.write(buffer);
    buffer.clear();
  }

  private static void putEntry(ByteBuffer buffer, CharSequence word, PostingLocation location, CharsetEncoder encoder) {
    final int lengthPlaceHolder = buffer.position();
    buffer.put((byte) 0);
    encoder.encode(CharBuffer.wrap(word), buffer, false);
    final byte length = (byte) (buffer.position() - lengthPlaceHolder - 1);

    buffer.put(lengthPlaceHolder, length);
    buffer.putLong(location.offset);
    buffer.putLong(location.length);
  }

  public static Vocabulary loadFrom(FileChannel channel, CharsetDecoder decoder) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
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

    destination.put(word.toString(), new PostingLocation(offset, length));
  }
}
