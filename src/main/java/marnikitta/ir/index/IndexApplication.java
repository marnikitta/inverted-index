package marnikitta.ir.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public final class IndexApplication {
  private final Path root;
  private final FileIndexer indexer = new FileIndexer();

  public IndexApplication(Path root) {
    this.root = root;
  }

  public static void main(String... args) throws IOException {
    final long start = System.currentTimeMillis();
    final Path root = Paths.get("");

    if (!Files.isDirectory(root)) {
      throw new IllegalArgumentException("First arg should be a path");
    }
    new IndexApplication(root).walkAndIndex();
    System.out.println(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
  }

  public void walkAndIndex() throws IOException {
    Files.walk(this.root).filter(Files::isRegularFile).forEach(p -> {
              try {
                this.indexer.indexFile(p);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
    );
  }
}
