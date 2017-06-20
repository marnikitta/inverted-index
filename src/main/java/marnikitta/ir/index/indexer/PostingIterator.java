package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;

public final class PostingIterator {
  private final FileChannel channel;
  private final ByteBuffer buffer = ByteBuffer.allocate(8192);

  private final long size;
  private final long offset;

  private boolean peeked = false;
  private long peeeeeek = Long.MAX_VALUE;

  private int readCount = 0;

  public PostingIterator(PostingLocation location, FileChannel channel) {
    this.size = location.length / 8;
    this.offset = location.offset;
    this.channel = channel;

    this.buffer.flip();
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  public long peek() throws IOException {
    if (this.peeked) {
      return this.peeeeeek;
    } else {
      if (this.readCount >= this.size) {
        throw new NoSuchFileException("End of posting");
      }

      if (this.buffer.remaining() > 0) {
        this.readCount++;
        this.peeked = true;
        this.peeeeeek = this.buffer.getLong();
        return this.peeeeeek;
      } else {
        this.buffer.compact();
        this.channel.position(this.offset + (this.readCount << 3));
        this.channel.read(this.buffer);
        this.buffer.flip();
        return this.peek();
      }
    }
  }

  public long next() throws IOException {
    this.peek();
    this.peeked = false;
    return this.peeeeeek;
  }

  public void seekLower(long entry) throws IOException {
    while (this.hasNext() && this.peek() < entry) {
      this.next();
    }
  }

  boolean hasNext() {
    return this.readCount < this.size || this.peeked;
  }
}
