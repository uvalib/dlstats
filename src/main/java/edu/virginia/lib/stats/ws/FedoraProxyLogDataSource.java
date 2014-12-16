package edu.virginia.lib.stats.ws;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import edu.virginia.lib.stats.HitMap;

public class FedoraProxyLogDataSource implements DataSource {

    private static final String SESSIONS = "Interactive View Sessions";
    
    final Logger logger = LoggerFactory.getLogger(FedoraProxyLogDataSource.class);
    
    public static final SimpleDateFormat YEAR_MONTH = new SimpleDateFormat("yyyy-MM");

    private final Directory indexDir;
    private final Directory taxoDir;
    private final FacetsConfig config = new FacetsConfig();
    
    private final ClientFilter filter = new ConfirmedBotClientFilter();
    
    public FedoraProxyLogDataSource(final File logFile) throws IOException {
        File indexDirFile = new File(logFile.getName() + "-index");
        File taxoDirFile = new File(logFile.getName() + "-taxo-index");
        if (indexDirFile.exists() && taxoDirFile.exists()) {
            indexDir = new NIOFSDirectory(indexDirFile.toPath());
            taxoDir = new NIOFSDirectory(taxoDirFile.toPath());
            logger.debug("Using existing index");
        } else {
            indexDir = new NIOFSDirectory(indexDirFile.toPath());
            taxoDir = new NIOFSDirectory(taxoDirFile.toPath());
            logger.debug("Building index from " + logFile.getPath());
            IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()));
            DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
            logger.debug("Building index from " + logFile.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
            try {
                String line = null;
                int records = 0;
                int lines = 0;
                while ((line = reader.readLine()) != null) {
                    lines ++;
                    try {
                        FedoraProxyLogEntry e = new FedoraProxyLogEntry(line);
                        if (e.getType().equals(FedoraProxyLogEntry.AccessType.UNKNOWN) && e.getPid() != null && !e.getPid().trim().equals("")) {
                            System.err.println(line);
                        }
                        if (e.getPid() == null || e.getPid().trim().equals("")) {
                            continue;
                        }
                        Document doc = new Document();
                        doc.add(new FacetField("action_facet", e.getType().toString()));
                        doc.add(new StringField("action", e.getType().toString(), Store.YES));
                        final String month = YEAR_MONTH.format(e.getDate());
                        if (line.contains("Jan") && !month.endsWith("-01")) {
                            throw new RuntimeException(month + " vs " + line);
                        }
                        doc.add(new StringField("month", month, Store.YES));
                        doc.add(new StringField("day", new SimpleDateFormat("dd").format(e.getDate()), Store.YES));
                        doc.add(new StringField("ip", e.getClientIPAddress(), Store.YES));
                        doc.add(new StringField("pid", e.getPid(), Store.YES));
                        indexWriter.addDocument(config.build(taxoWriter, doc));
                        records ++;
                    } catch (RuntimeException ex) {
                        ex.printStackTrace();
                        System.out.println("Skipping: " + line);
                        System.exit(-1);
                    }
                }
                indexWriter.close();
                taxoWriter.close();
                System.out.println("Index built from " + logFile.getPath() + " with " + records + " records parsed from " + lines + " lines.");
            } finally {            
                reader.close();
            }
        }
    }
    
    public String getId() {
        return "fedora-proxy.lib.virginia.edu logs";
    }

    public String getStartingAvailability() {
        return "2014-11";
    }

    public String getEndingAvailability() {
        return "2016-07";
    }
    
    public String toString() {
        return getId() + " (" + getStartingAvailability() + " to " + getEndingAvailability() + ")";
    }

    public void getActionCountsPerMonth(String month, final Collection c, final HitMap actions, final boolean excludeKnownBots) {
        try {
            /**
             * Keeps track of interactive views, which represents client region requests per day.  
             * The values in this map are integers where bits 1-31 represent days of month in which
             * the client requested at least one tile.
             */
            final Map<String, Integer> clientToDaysMap = new HashMap<String, Integer>();
            final DirectoryReader indexReader = DirectoryReader.open(indexDir);
            final IndexSearcher searcher = new IndexSearcher(indexReader);
    
            searcher.search(new TermQuery(new Term("month", month)), new Collector() {

                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    return new LeafCollector() {

                        public void setScorer(Scorer scorer) throws IOException {
                        }

                        public void collect(int doc) throws IOException {
                            Document d = indexReader.document(doc);
                            String pid = d.get("pid");
                            String client = d.get("ip");
                            if (c.isIdInCollection(pid)) {
                                String[] values = d.getValues("action");
                                if (values != null) {
                                    for (String v : values) {
                                        if (v.equals(FedoraProxyLogEntry.AccessType.REGION.toString())) {
                                            int day = 1 << Integer.parseInt(d.get("day"));
                                            Integer days = clientToDaysMap.get(client);
                                            if (days == null) {
                                                days = day;
                                            } else {
                                                days |= day;
                                            }
                                            clientToDaysMap.put(client, days);
                                        } else {
                                            if (!excludeKnownBots || filter.include(client)) {
                                                actions.hit(v);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    };
                }
                
                public boolean needsScores() {
                    return false;
                }});
        
            for (Map.Entry<String, Integer> entry : clientToDaysMap.entrySet()) {
               actions.hit(SESSIONS, Integer.bitCount(entry.getValue()));
            }
            indexReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void getPopularItemCountsForMonth(String month, final Collection c, final Set<FedoraProxyLogEntry.AccessType> actionTypes, final HitMap items, final boolean excludeKnownBots) {
        try {
            final DirectoryReader indexReader = DirectoryReader.open(indexDir);
            final IndexSearcher searcher = new IndexSearcher(indexReader);

            searcher.search(new TermQuery(new Term("month", month)), new Collector() {

                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    return new LeafCollector() {
    
                        public void setScorer(Scorer scorer) throws IOException {
                        }
    
                        public void collect(int doc) throws IOException {
                            Document d = indexReader.document(doc);
                            String pid = d.get("pid");
                            String client = d.get("ip");
                            if (c.isIdInCollection(pid)) {
                                String[] values = d.getValues("action");
                                if (values != null) {
                                    for (String v : values) {
                                        FedoraProxyLogEntry.AccessType t = FedoraProxyLogEntry.AccessType.fromString(v);
                                        if (actionTypes.contains(t)) { 
                                            if (!excludeKnownBots || filter.include(client)) {
                                                items.hit(pid);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    };
                }
                
                public boolean needsScores() {
                    return false;
                }});

            indexReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

}
