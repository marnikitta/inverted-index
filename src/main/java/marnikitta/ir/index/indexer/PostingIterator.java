package marnikitta.ir.index.indexer;

import marnikitta.ir.index.encoder.PostingDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;

public final class PostingIterator {
  public static final int MAX_BLOCK_LENGTH = 30;

  private final FileChannel channel;
  private final ByteBuffer buffer = ByteBuffer.allocate(8192);
  private final LongBuffer longBuffer = LongBuffer.allocate(8192);
  private final PostingDecoder decoder = new PostingDecoder();

  private final PostingLocation location;

  private long fileBytesRead = 0L;

  private boolean peeked = false;
  private long peeeeeek = 0L;

  public PostingIterator(PostingLocation location, FileChannel channel) {
    this.location = location;
    this.channel = channel;

    this.buffer.flip();
    this.longBuffer.flip();
  }

  public long peek() throws IOException {
    if (this.peeked) {
      return this.peeeeeek;
    } else {
      if (this.longBuffer.remaining() == 0) {
        this.refillBuffer();
      }

      this.peeeeeek = this.longBuffer.get();
      this.peeked = true;
      return this.peeeeeek;
    }
  }

  private void refillBuffer() throws IOException {
    this.channel.position(this.location.offset + this.fileBytesRead);
    final long remainingToRead = this.location.length - this.fileBytesRead;

    if (remainingToRead <= 0) {
      throw new NoSuchElementException();
    }

    this.buffer.compact();
    this.longBuffer.compact();

    if (this.buffer.remaining() > remainingToRead) {
      this.buffer.limit((int) (this.buffer.position() + remainingToRead));
    }

    final int bytesRead = this.channel.read(this.buffer);
    if (bytesRead == -1) {
      throw new IllegalStateException();
    }
    this.fileBytesRead += bytesRead;

    this.buffer.flip();
    this.decoder.decode(this.buffer, this.longBuffer, this.peeeeeek);
    this.longBuffer.flip();
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
    return this.fileBytesRead < this.location.length || this.peeked || this.longBuffer.remaining() > 0;
  }
}
