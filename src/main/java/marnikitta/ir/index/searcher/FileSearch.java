package marnikitta.ir.index.searcher;

import marnikitta.ir.index.DocIder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public final class FileSearch {
  private final Index index;
  private final DocIder ider;

  public FileSearch(Index index, DocIder ider) {
    this.index = index;
    this.ider = ider;
  }

  public Set<Path> intersect(Collection<? extends CharSequence> query, int limit) throws IOException {
    final Set<Integer> docIds = this.index.intersect(query, limit);

    final Set<Path> result = new HashSet<>();
    for (int docId : docIds) {
      result.add(this.ider.get(docId).orElseThrow(() -> new NoSuchElementException("Do document with id " + docId)));
    }
    return result;
  }
}
