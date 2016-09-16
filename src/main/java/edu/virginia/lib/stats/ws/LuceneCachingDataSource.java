package edu.virginia.lib.stats.ws;

import edu.virginia.lib.stats.HitMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class LuceneCachingDataSource implements DataSource {

    private static final String FEDORA_PROD_02 = "128.143.23.192";

    private final ClientFilter filter = new ConfirmedBotClientFilter();

    protected final LuceneCache cache;

    public abstract String getId();

    public abstract String getStartingAvailability();

    public abstract String getEndingAvailability();

    public LuceneCachingDataSource() throws IOException {
        cache = new LuceneCache(this.getClass().getSimpleName());
    }

    @Override
    public void getActionCountsPerMonth(String month, final Collection c, final HitMap actions, final boolean excludeKnownBots) {
        try {
            /**
             * Keeps track of interactive views, which represents client region requests per day.
             * The values in this map are integers where bits 1-31 represent days of month in which
             * the client requested at least one tile.
             */
            final Map<String, Integer> clientToDaysMap = new HashMap<String, Integer>();
            final DirectoryReader indexReader = cache.getDirectoryReader();
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
                                        if (v.equals(AccessType.REGION.toString())) {
                                            int day = 1 << Integer.parseInt(d.get("day"));
                                            Integer days = clientToDaysMap.get(client);
                                            if (days == null) {
                                                days = day;
                                            } else {
                                                days |= day;
                                            }
                                            clientToDaysMap.put(client, days);
                                        } else {
                                            if (!pid.equals(FEDORA_PROD_02) || (!excludeKnownBots || filter.include(client))) {
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
                actions.hit(DataSource.AccessType.SESSIONS.toString(), Integer.bitCount(entry.getValue()));
            }
            indexReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getPopularItemCountsForMonth(String month, final Collection c, final Set<AccessType> actionTypes, final HitMap items, final boolean excludeKnownBots) {
        try {
            final DirectoryReader indexReader = cache.getDirectoryReader();
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
                                        AccessType t = AccessType.fromString(v);
                                        if (actionTypes.contains(t)) {
                                            if (!pid.equals(FEDORA_PROD_02) || (!excludeKnownBots || filter.include(client))) {
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
