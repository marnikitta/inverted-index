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

public final class FileIndexer {
  private final Index index = new Index(Paths.get(""));
  private final DocIder docIder = new DocIder();

  private final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
  private final ByteBuffer slice = this.buffer.asReadOnlyBuffer();

  private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  public void indexFile(Path path) throws IOException {
    final int docId = this.docIder.register(path);

    try (FileChannel fc = FileChannel.open(path)) {
      this.buffer.clear();

      final int position = (int) fc.position();

      while (fc.read(this.buffer) > 0) {
        this.buffer.flip();

        int start = 0;
        while (this.buffer.hasRemaining()) {
          final byte current = this.buffer.get();
          if (!this.isValidChar(current)) {
            final int pos = this.buffer.position() - 1;
            if (pos > start && pos - start < 100) {
              this.slice.limit(pos).position(start);
              final CharBuffer word = this.decoder.decode(this.slice);
              this.index.index(word, docId, position + start);
            }
            start = pos + 1;
          } else {
            this.buffer.put(this.buffer.position() - 1, current >= 'A' && current <= 'Z' ? (byte) (current - (byte) 'A' + (byte) 'a') : current);
          }
        }

        this.buffer.clear();
      }
    }
  }
  //this.indexChunk.spill(
  //        FileChannel.open(Paths.get("1.vcb"), StandardOpenOption.CREATE, StandardOpenOption.WRITE),
  //        FileChannel.open(Paths.get("1.pst"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
  //);

  private boolean isValidChar(byte current) {
    return current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z';
  }
}
