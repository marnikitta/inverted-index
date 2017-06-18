package marnikitta.ir.index.searcher;

import marnikitta.ir.index.indexer.PostingLocation;
import marnikitta.ir.index.indexer.Vocabulary;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public final class SearchApplication {
  private final Path vocab;
  private final Path postings;

  public SearchApplication(Path vocab, Path postings) {
    this.vocab = vocab;
    this.postings = postings;
  }

  public static void main(String... args) {
    new SearchApplication(Paths.get("0.vcb"), Paths.get("0.pst")).run();
  }

  public void run() {
    try {
      try (FileChannel fc = FileChannel.open(this.vocab)) {
        final Vocabulary vocabulary = Vocabulary.load(fc, StandardCharsets.UTF_8.newDecoder());

        final Map<CharSequence, PostingLocation> voc = vocabulary.payload();
        voc.size();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
