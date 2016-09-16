package edu.virginia.lib.stats.ws;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraCredentials;
import edu.virginia.lib.TracksysClient;
import edu.virginia.lib.indexing.FedoraRiSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by md5wz on 10/27/16.
 */
public class SolrQueryCollection implements Collection {

    final static Logger LOGGER = LoggerFactory.getLogger(SolrQueryCollection.class);

    /**
     * The index is built to answer the following two questions:
     * 1.  What pids are part of a given pid
     * 2.  What pids are flagged as part of the given collection.
     *
     * The document format is simple:
     *   pid
     *   parentPid
     *   collection (multivalued)
     */
    LuceneCache cache;

    private String collection;

    private String name;

    public SolrQueryCollection(final String name, final String collection, final String query, final TracksysClient tracksys, final SolrServer solr, final String fedoraUrl) throws Exception {
        this.name = name;
        this.collection = collection;
        cache = new LuceneCache(this.getClass().getSimpleName());
        updatePidCache(query, collection, solr, tracksys, fedoraUrl);
    }

    private void updatePidCache(final String query, final String collection, final SolrServer solr, final TracksysClient tracksys, final String fedoraUrl) throws Exception {
        LOGGER.info("Updating cache of page pids [started]");
        FedoraClient fc = new FedoraClient(new FedoraCredentials(fedoraUrl, null, null));

        IndexSearcher s = null;
        try {
            s = new IndexSearcher(cache.getDirectoryReader());
        } catch (IndexNotFoundException e) {
            // we're starting fresh...
        }

        IndexWriter indexWriter = cache.getIndexWriter();
        int count = 0;
        try {
            Iterator<String> idIt = getIdsForQuery(solr, query);
            while (idIt.hasNext()) {
                final String pid = idIt.next();
                LOGGER.trace(++count + ", " + pid);
                if (isItemIndexed(s, pid)) {
                    markPidAndChildrenAsCollectionMember(s, indexWriter, pid, collection);
                } else {
                    List<String> pagePids;
                    try {
                        pagePids = tracksys.getPagePids(pid, false);
                    } catch (RuntimeException ex) {
                        LOGGER.trace(ex.getMessage() + ", checking fedora...");
                        pagePids = FedoraRiSearcher.getSubjects(fc, pid, "http://fedora.lib.virginia.edu/relationships#isDigitalRepresentationOf");
                    }
                    LOGGER.trace("  ... found + " + pagePids.size() + " pages.");
                    for (String childPid : pagePids) {
                        addChildPid(indexWriter, childPid, pid, collection);
                    }
                    addChildPid(indexWriter, pid, null, collection);
                    indexWriter.commit();
                    indexWriter.flush();

                }
            }
        } finally {
            indexWriter.close();
            LOGGER.info("Updating cache of page pids [complete]");
        }
    }

    void addChildPid(IndexWriter w, final String pid, final String parentPid, final String collection) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("pid", pid, Field.Store.YES));
        if (parentPid != null) {
            doc.add(new StringField("parentPid", parentPid, Field.Store.YES));
        }
        doc.add(new StringField("collection", collection, Field.Store.YES));
        LOGGER.trace("Added " + pid + " of " + parentPid + " in " + collection);
        w.addDocument(doc);
    }

    boolean isItemIndexed(final IndexSearcher s, final String pid) throws IOException {
        if (s == null) {
            return false;
        }
        TotalHitCountCollector c = new TotalHitCountCollector();
        s.search(new TermQuery(new Term("pid", pid)), c);
        return (c.getTotalHits() > 0);
    }

    void markPidAndChildrenAsCollectionMember(final IndexSearcher s, final IndexWriter w, final String pid, final String collection) throws IOException {
        s.search(new TermQuery(new Term("parentPid", pid)), new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                return new AddCollectionLeafCollector(s.getIndexReader(), w, collection);
            }

            @Override
            public boolean needsScores() {
                return false;
            }
        });
    }

    private static class AddCollectionLeafCollector implements LeafCollector {

        private IndexReader reader;
        private IndexWriter writer;
        private String collection;

        public AddCollectionLeafCollector(final IndexReader reader, final IndexWriter writer, final String collection) {
            this.reader = reader;
            this.writer = writer;
            this.collection = collection;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            Document document = reader.document(doc);
            IndexableField fields[] = document.getFields("collection");
            if (fields != null) {
                for (IndexableField f : fields) {
                    if (f.stringValue().equals(collection)) {
                        LOGGER.trace(" ... is already part of collection");
                        return;
                    }
                }
            }
            document.add(new StringField("collection", collection, Field.Store.YES));
            writer.addDocument(document);
        }
    }

    private Iterator<String> getIdsForQuery(final SolrServer solr, final String query) throws SolrServerException {
        int start = 0;
        final ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", new String[] { query });
        p.set("rows", 100);
        p.set("start", start);
        return new Iterator<String>() {

            int index = 0;
            int start = 0;
            QueryResponse response = null;

            public boolean hasNext() {
                if (response == null || response.getResults().size() <= index) {
                    p.set("rows", 100);
                    p.set("start", start);
                    try {
                        response = solr.query(p);
                        start += response.getResults().size();
                        index = 0;
                    } catch (SolrServerException e) {
                        throw new RuntimeException(e);
                    }
                }
                return response.getResults().size() > index;
            }

            public String next() {
                if (!hasNext()) {
                    throw new IllegalStateException();
                }
                return String.valueOf(response.getResults().get(index ++).getFirstValue("id"));
            }
        };
    }

    public SolrQueryCollection(final String solrQuery) {

    }

    @Override
    public String getName() {
        return name;
    }

    private IndexSearcher s = null;

    @Override
    public boolean isIdInCollection(String id) {
        try {
            if (s == null) {
                s = new IndexSearcher(cache.getDirectoryReader());
            }
            TotalHitCountCollector c = new TotalHitCountCollector();
            s.search(new BooleanQuery.Builder().add(new TermQuery(new Term("pid", id)), BooleanClause.Occur.MUST).add(new TermQuery(new Term("collection", collection)), BooleanClause.Occur.MUST).build(), c);
            return c.getTotalHits() == 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
