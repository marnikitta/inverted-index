package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class Indexer {
  private static final int CHUNK_THRESHOLD = 1 << 26;
  private int currentChunkId = 0;
  private IndexChunk currentIndex = new IndexChunk();
  private final Path idxDir;

  public Indexer(Path dir) {
    this.idxDir = dir;
  }

  public void index(CharSequence word, int docId, int position) throws IOException {
    if (this.currentIndex.size() >= Indexer.CHUNK_THRESHOLD) {
      this.rotate();
    }

    this.currentIndex.append(word, docId, position);
    // TODO: 6/21/17 spill last chunk
  }

  private void rotate() throws IOException {
    System.out.println("Spilling: " + System.currentTimeMillis());

    final Path vocabPath = this.idxDir.resolve(this.currentChunkId + ".vcb");
    final Path postingPath = this.idxDir.resolve(this.currentChunkId + ".pst");

    try (FileChannel vocab = FileChannel.open(vocabPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
         FileChannel posting = FileChannel.open(postingPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      final Vocabulary vocabulary = SpilledPosting.spill(this.currentIndex, posting);
      vocabulary.spill(vocab, StandardCharsets.UTF_8.newEncoder());
    }

    this.currentIndex = new IndexChunk();
    this.currentChunkId++;

    System.out.println("Spilling done: " + System.currentTimeMillis());
  }
}
