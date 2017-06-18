package marnikitta.ir.index.indexer;

public final class PostingLocation {
  public final long offset;

  public final long length;

  public PostingLocation(long offset, long length) {
    this.offset = offset;
    this.length = length;
  }
}
