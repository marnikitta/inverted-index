package marnikitta.ir.index.indexer;

import marnikitta.ir.index.DocIder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class FileIndexer {
  private final Indexer indexer = new Indexer(Paths.get(""));
  private final DocIder docIder = new DocIder();

  private final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
  private final ByteBuffer slice = this.buffer.asReadOnlyBuffer();

  private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  public void indexFile(Path path) throws IOException {
    final int docId = this.docIder.register(path);

    try (FileChannel fc = FileChannel.open(path)) {
      this.buffer.clear();

      while (fc.read(this.buffer) > 0) {
        final int filePosition = (int) (fc.position() - this.buffer.limit());
        this.buffer.flip();

        int start = 0;
        while (this.buffer.hasRemaining()) {
          final byte current = this.buffer.get();

          if (FileIndexer.isValidChar(current)) {
            this.buffer.put(this.buffer.position() - 1, current >= 'A' && current <= 'Z' ? (byte) (current - (byte) 'A' + (byte) 'a') : current);
          } else {
            final int pos = this.buffer.position() - 1;
            if (pos > start && pos - start < 100) {
              this.slice.limit(pos).position(start);
              final CharBuffer word = this.decoder.decode(this.slice);
              this.indexer.index(word, docId, filePosition + start);
            }
            start = pos + 1;
          }
        }

        this.buffer.clear();
      }
    }
  }

  public void spillDocIds(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      this.docIder.spill(channel, StandardCharsets.UTF_8.newEncoder());
    }
  }

  private static boolean isValidChar(byte current) {
    return current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z';
  }
}
