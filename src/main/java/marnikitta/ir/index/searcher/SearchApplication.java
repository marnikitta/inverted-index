package marnikitta.ir.index.searcher;

import marnikitta.ir.index.DocIder;
import marnikitta.ir.index.indexer.SpilledPosting;
import marnikitta.ir.index.indexer.Vocabulary;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class SearchApplication {
  private final FileSearch search;

  public SearchApplication(FileSearch search) {
    this.search = search;
  }

  public static void main(String... args) throws IOException {
    try (FileChannel vocChannel = FileChannel.open(Paths.get("0.vcb"));
         FileChannel docChannel = FileChannel.open(Paths.get("docIds.dcd"));
         FileChannel postingChannel = FileChannel.open(Paths.get("0.pst"))) {

      final SpilledPosting posting = new SpilledPosting(postingChannel);
      final DocIder docIder = DocIder.load(docChannel, StandardCharsets.UTF_8.newDecoder());
      final Vocabulary vocabulary = Vocabulary.loadFrom(vocChannel, StandardCharsets.UTF_8.newDecoder());
      final Index index = new Index(vocabulary, posting);
      new SearchApplication(new FileSearch(index, docIder)).run();
    }
  }

  public void run() throws IOException {
    this.test1();
    this.test2();
    this.test3();
    this.test4();
    System.out.println("win");
  }

  private void test1() throws IOException {
    this.assertIntersect("peckham armitage baldwin", "wiki/wiki-part.xml.fk, wiki/wiki-part.xml.by, wiki/wiki-part.xml.bl, wiki/wiki-part.xml.fq, wiki/wiki-part.xml.yv, wiki/wiki-part.xml.wi, wiki/wiki-part.xml.zame");
  }

  private void test2() throws IOException {
    this.assertIntersect("wolseley monica chrisbarrie", "wiki/wiki-part.xml.zame");
  }

  private void test3() throws IOException {
    this.assertIntersect("again uses superlatives", "wiki/wiki-part.xml.bw, wiki/wiki-part.xml.mc, wiki/wiki-part.xml.qg, wiki/wiki-part.xml.of, wiki/wiki-part.xml.le, wiki/wiki-part.xml.my, wiki/wiki-part.xml.bo, wiki/wiki-part.xml.ix, wiki/wiki-part.xml.bs, wiki/wiki-part.xml.fw, wiki/wiki-part.xml.zame, wiki/wiki-part.xml.pz, wiki/wiki-part.xml.aa, wiki/wiki-part.xml.km, wiki/wiki-part.xml.df, wiki/wiki-part.xml.sv, wiki/wiki-part.xml.lo");
  }

  private void test4() throws IOException {
    this.assertIntersect("chord and a grand sweep of arpeggios", "wiki/wiki-part.xml.dy, wiki/wiki-part.xml.vl, wiki/wiki-part.xml.fp, wiki/wiki-part.xml.kx, wiki/wiki-part.xml.ts, wiki/wiki-part.xml.zame");
  }

  private void assertIntersect(String q, String p) throws IOException {
    final Set<String> query = SearchApplication.parseQuery(q);
    final Set<Path> expected = SearchApplication.parseLine(p);
    final Set<Path> got = this.search.intersect(query, 100);

    System.out.printf("# Test\nQuery: %s\nExpected: %s\nGot: %s\n", query, expected, got);
    if (!expected.equals(got)) {
      throw new IllegalArgumentException("Test failed");
    }
  }

  private static Set<String> parseQuery(String str) {
    return Arrays.stream(str.split("\\s")).filter(s -> !s.isEmpty()).map(String::toLowerCase).collect(Collectors.toSet());
  }

  private static Set<Path> parseLine(String line) {
    return Arrays.stream(line.split("[\\,\\s]")).filter(s -> !s.isEmpty()).map(Paths::get).collect(Collectors.toSet());
  }
}
