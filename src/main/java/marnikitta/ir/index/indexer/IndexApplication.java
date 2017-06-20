package marnikitta.ir.index.indexer;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    System.out.println("Start: " + System.currentTimeMillis());
    final long start = System.currentTimeMillis();

    final Path root = Paths.get(args[0]);

    if (!Files.isDirectory(root)) {
      throw new IllegalArgumentException("First arg should be a path");
    }
    new IndexApplication(root).walkAndIndex();

    System.out.println(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
    System.out.println("Stop: " + System.currentTimeMillis());
  }

  public void walkAndIndex() throws IOException {
    Files.walk(this.root).filter(Files::isRegularFile).forEach(p -> {
              try {
                this.indexer.indexFile(p);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
    );
    this.indexer.spillDocIds(Paths.get("docIds.dcd"));
  }
}
