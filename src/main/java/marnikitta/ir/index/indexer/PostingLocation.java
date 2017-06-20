package marnikitta.ir.index.indexer;

import java.util.Objects;

public final class PostingLocation {
  public final long offset;

  public final long length;

  public PostingLocation(long offset, long length) {
    this.offset = offset;
    this.length = length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final PostingLocation location = (PostingLocation) o;
    return this.offset == location.offset &&
            this.length == location.length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.offset, this.length);
  }
}
