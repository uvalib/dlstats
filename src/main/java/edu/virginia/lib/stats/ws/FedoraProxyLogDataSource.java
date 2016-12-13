package edu.virginia.lib.stats.ws;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

public class FedoraProxyLogDataSource extends LuceneCachingDataSource {

    final Logger logger = LoggerFactory.getLogger(FedoraProxyLogDataSource.class);
    
    public static final SimpleDateFormat YEAR_MONTH = new SimpleDateFormat("yyyy-MM");

    public FedoraProxyLogDataSource(final File logFile) throws IOException {
        super();
        if (!cache.isEmpty()) {
            logger.debug("Using existing index");
        } else {
            IndexWriter indexWriter = cache.getIndexWriter();
            TaxonomyWriter taxoWriter = cache.getTaxonomyWriter();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
            logger.info("Indexing " + logFile.getName() + " [started]");
            try {
                String line = null;
                int records = 0;
                int lines = 0;
                while ((line = reader.readLine()) != null) {
                    lines ++;
                    try {
                        FedoraProxyLogEntry e = new FedoraProxyLogEntry(line);
                        if (e.getType().equals(DataSource.AccessType.UNKNOWN) && e.getPid() != null && !e.getPid().trim().equals("")) {
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
                        indexWriter.addDocument(cache.getFacetsConfig().build(taxoWriter, doc));
                        records ++;
                    } catch (RuntimeException ex) {
                        ex.printStackTrace();
                        logger.debug("Skipping: " + line);
                        //System.exit(-1);
                    }
                }
                indexWriter.close();
                taxoWriter.close();
                logger.debug("Index built from " + logFile.getPath() + " with " + records + " records parsed from " + lines + " lines.");
            } finally {            
                reader.close();
                logger.info("Indexing " + logFile.getName() + " [started]");
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
        return "2016-11";
    }
    
    public String toString() {
        return getId() + " (" + getStartingAvailability() + " to " + getEndingAvailability() + ")";
    }

}
