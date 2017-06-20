package marnikitta.ir.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;

public final class DocIder {
  private static final int MAX_ENTRY_SIZE = 300;

  private Path[] paths = new Path[1];
  private int size = 0;

  public DocIder() {
  }

  public DocIder(Path[] paths) {
    this.paths = Arrays.copyOf(paths, paths.length);
  }

  public int register(Path path) {
    if (this.paths.length == this.size) {
      this.paths = Arrays.copyOf(this.paths, this.paths.length << 1);
    }

    this.paths[this.size] = path;
    this.size++;
    return this.size - 1;
  }

  public void spill(FileChannel channel, CharsetEncoder encoder) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);

    for (int i = 0; i < this.size; ++i) {
      if (buffer.remaining() <= DocIder.MAX_ENTRY_SIZE) {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
      }

      final int lengthPlaceHolder = buffer.position();
      buffer.put((byte) 0);
      encoder.encode(CharBuffer.wrap(this.paths[i].toString()), buffer, false);
      final byte length = (byte) (buffer.position() - lengthPlaceHolder - 1);
      buffer.put(lengthPlaceHolder, length);
    }

    buffer.flip();
    channel.write(buffer);
    buffer.clear();
  }

  public static DocIder load(FileChannel channel, CharsetDecoder decoder) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(8192);
    final ByteBuffer slice = buffer.asReadOnlyBuffer();
    final DocIder result = new DocIder();

    while (channel.read(buffer) > 0) {
      buffer.flip();
      while (buffer.remaining() > DocIder.MAX_ENTRY_SIZE) {
        DocIder.readInto(result, buffer, slice, decoder);
      }
      buffer.compact();
    }

    buffer.flip();

    while (buffer.remaining() > 0) {
      DocIder.readInto(result, buffer, slice, decoder);
    }
    return result;
  }

  public Optional<Path> get(int docId) {
    if (docId >= this.size || docId < 0) {
      return Optional.empty();
    } else {
      return Optional.of(this.paths[docId]);
    }
  }

  private static void readInto(DocIder result,
                               ByteBuffer buffer,
                               ByteBuffer slice,
                               CharsetDecoder decoder) throws CharacterCodingException {
    final byte wordLength = buffer.get();

    slice.limit(buffer.position() + wordLength).position(buffer.position());
    final CharBuffer word = decoder.decode(slice);
    buffer.position(buffer.position() + wordLength);

    result.register(Paths.get(word.toString()));
  }
}
