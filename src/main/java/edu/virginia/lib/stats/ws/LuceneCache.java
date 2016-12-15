package edu.virginia.lib.stats.ws;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by md5wz on 10/25/16.
 */
public class LuceneCache {

    final Logger logger = LoggerFactory.getLogger(LuceneCache.class);

    private File indexDirFile;
    private File taxoDirFile;

    private final Directory indexDir;
    private final Directory taxoDir;
    private final FacetsConfig config = new FacetsConfig();

    private boolean empty = false;

    public LuceneCache(final String cacheName) throws IOException {
        indexDirFile = new File(cacheName + "-index");
        taxoDirFile = new File(cacheName + "-taxo-index");
        if (indexDirFile.exists() && taxoDirFile.exists()) {
            empty = false;
            logger.debug("Using existing index");
        } else {
            empty = true;
        }
        indexDir = new NIOFSDirectory(indexDirFile.toPath());
        taxoDir = new NIOFSDirectory(taxoDirFile.toPath());
    }

    public void deleteIndex() throws IOException {
        FileUtils.deleteDirectory(indexDirFile);
        FileUtils.deleteDirectory(indexDirFile);
    }

    public IndexWriter getIndexWriter() throws IOException {
        return new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()));
    }

    public TaxonomyWriter getTaxonomyWriter() throws IOException {
        return new DirectoryTaxonomyWriter(taxoDir);
    }

    public DirectoryReader getDirectoryReader() throws IOException {
        return DirectoryReader.open(indexDir);
    }

    public FacetsConfig getFacetsConfig() {
        return this.config;
    }

    public boolean isEmpty() {
        return empty;
    }

    public int docCount() throws IOException {
        return docCount(new MatchAllDocsQuery());
    }

    public int docCount(Query query) throws IOException {
        final DirectoryReader indexReader = getDirectoryReader();
        try {
            final IndexSearcher searcher = new IndexSearcher(indexReader);
            TotalHitCountCollector hits = new TotalHitCountCollector();
            searcher.search(query, hits);
            return hits.getTotalHits();
        } finally {
            indexReader.close();
        }
    }

}
