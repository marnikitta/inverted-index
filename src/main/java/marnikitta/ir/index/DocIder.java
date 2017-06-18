package marnikitta.ir.index;

import java.nio.file.Path;
import java.util.Arrays;

public final class DocIder {
  private Path[] paths = new Path[1];
  private int size = 0;

  public int register(Path path) {
    if (this.paths.length == this.size) {
      this.paths = Arrays.copyOf(this.paths, this.paths.length << 1);
    }

    this.paths[this.size] = path;
    this.size++;
    return this.size - 1;
  }
}
