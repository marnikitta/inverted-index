package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class Index {
  private static final int CHUNK_THRESHOLD = 1 << 26;
  private int currentChunkId = 0;
  private IndexChunk currentIndex = new IndexChunk();
  private final Path idxDir;

  private final List<IndexDescriptor> chunks = new ArrayList<>();

  public Index(Path idxDir) {
    this.idxDir = idxDir;
  }

  public void index(CharSequence word, int docId, int position) throws IOException {
    if (this.currentIndex.size() >= Index.CHUNK_THRESHOLD) {
      this.rotate();
    }

    this.currentIndex.append(word, docId, position);
  }

  private void rotate() throws IOException {
    final Path vocabPath = this.idxDir.resolve(this.currentChunkId + ".vcb");
    final Path postingPath = this.idxDir.resolve(this.currentChunkId + ".pst");


    try (FileChannel vocab = FileChannel.open(vocabPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
         FileChannel posting = FileChannel.open(postingPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      System.out.println("Spilling: " + System.currentTimeMillis());
      new ChunkSpiller(this.currentIndex).spill(vocab, posting);
      System.out.println("Spilling done: " + System.currentTimeMillis());
    }

    this.chunks.add(new IndexDescriptor(postingPath, vocabPath));
    this.currentIndex = new IndexChunk();
    this.currentChunkId++;
  }

  public class IndexDescriptor {
    public final Path postings;
    public final Path vocabulary;

    public IndexDescriptor(Path postings, Path vocabulary) {
      this.postings = postings;
      this.vocabulary = vocabulary;
    }
  }
}
