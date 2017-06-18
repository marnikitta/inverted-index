package marnikitta.ir.index.searcher;

import marnikitta.ir.index.indexer.PostingLocation;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class Searcher {
  private final Path vocPath;
  private final Path postingsPath;

  private final Map<CharSequence, PostingLocation> vocabulary = new HashMap<>();

  public Searcher(Path vocPath, Path postingsPath) {
    this.vocPath = vocPath;
    this.postingsPath = postingsPath;
  }

  public void firstEntry(CharSequence word) {
  }

  public static class PostingEntry {
    public final int docId;
    public final int position;

    public PostingEntry(int docId, int position) {
      this.docId = docId;
      this.position = position;
    }
  }
}
