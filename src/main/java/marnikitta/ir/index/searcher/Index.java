package marnikitta.ir.index.searcher;

import marnikitta.ir.index.indexer.PostingLocation;
import marnikitta.ir.index.indexer.SpilledPosting;
import marnikitta.ir.index.indexer.Vocabulary;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public final class Index {
  private final Vocabulary vocabulary;
  private final SpilledPosting posting;

  public Index(Vocabulary vocabulary, SpilledPosting posting) {
    this.vocabulary = vocabulary;
    this.posting = posting;
  }

  /**
   * @throws IllegalArgumentException
   */
  public Set<Integer> intersect(Collection<? extends CharSequence> query, int limit) throws IOException {
    final Collection<PostingLocation> locations = new HashSet<>();

    for (CharSequence seq : query) {
      final PostingLocation location = this.vocabulary.get(seq)
              .orElseThrow(() -> new IllegalArgumentException("No such word '" + seq + "' in vocabulary"));
      locations.add(location);
    }

    return this.posting.intersect(locations, limit);
  }
}
