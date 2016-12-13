package edu.virginia.lib.stats.ws;

import edu.virginia.lib.stats.IIIFLogEntry;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MicroServiceLogDataSource extends LuceneCachingDataSource {

    private static final String UNKNOWN_IP = "0.0.0.0";

    final Logger logger = LoggerFactory.getLogger(MicroServiceLogDataSource.class);

    public static final SimpleDateFormat YEAR_MONTH = new SimpleDateFormat("yyyy-MM");

    public MicroServiceLogDataSource(final File iiifLog, final File pdfLog, final File rwLog) throws IOException, ParseException {
        super();
        if (cache.isEmpty()) {

            IndexWriter indexWriter = cache.getIndexWriter();
            TaxonomyWriter taxoWriter = cache.getTaxonomyWriter();

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(iiifLog)));
            logger.info("Parsing " + iiifLog.getName() + " [started]");
            try {
                String line = null;
                int records = 0;
                int lines = 0;
                while ((line = reader.readLine()) != null) {
                    lines++;
                    try {
                        IIIFLogEntry e = new IIIFLogEntry(line);
                        if (e.getType().equals(AccessType.UNKNOWN) && e.getPid() != null && !e.getPid().trim().equals("")) {
                            System.err.println("UKNOWN: " + line);
                        }
                        if (e.getPid() == null || e.getPid().trim().equals("")) {
                            continue;
                        }
                        Document doc = new Document();
                        doc.add(new FacetField("action_facet", e.getType().toString()));
                        doc.add(new StringField("action", e.getType().toString(), Store.YES));
                        final String month = YEAR_MONTH.format(e.getDate());
                        doc.add(new StringField("month", month, Store.YES));
                        doc.add(new StringField("day", new SimpleDateFormat("dd").format(e.getDate()), Store.YES));
                        doc.add(new StringField("ip", e.getClientIPAddress(), Store.YES));
                        doc.add(new StringField("pid", e.getPid(), Store.YES));
                        indexWriter.addDocument(cache.getFacetsConfig().build(taxoWriter, doc));
                        records++;
                    } catch (RuntimeException ex) {
                        logger.debug("Skipping: " + line);
                    }
                }

                System.out.println("Index built from " + iiifLog.getName() + " with " + records + " records parsed from " + lines + " lines.");
            } finally {
                reader.close();
                logger.info("Parsing " + iiifLog.getName() + " [completed]");
            }

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(pdfLog)));
            logger.info("Parsing " + pdfLog.getName() + " [started]");
            Pattern pdfDownloadPattern = Pattern.compile("service: (\\d\\d\\d\\d/\\d\\d/\\d\\d) \\d\\d:\\d\\d:\\d\\d PDF for (.*) completed successfully");
            SimpleDateFormat PDF_LOG_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd");
            try {
                String line = null;
                int records = 0;
                int lines = 0;
                while ((line = reader.readLine()) != null) {
                    lines++;
                    Matcher m = pdfDownloadPattern.matcher(line);
                    if (m.matches()) {
                        Date date = PDF_LOG_DATE_FORMAT.parse(m.group(1));
                        Document doc = new Document();
                        doc.add(new FacetField("action_facet", DataSource.AccessType.PDF_DOWNLOAD.toString()));
                        doc.add(new StringField("action", DataSource.AccessType.PDF_DOWNLOAD.toString(), Store.YES));
                        final String month = YEAR_MONTH.format(date);
                        doc.add(new StringField("month", month, Store.YES));
                        doc.add(new StringField("day", new SimpleDateFormat("dd").format(date), Store.YES));
                        doc.add(new StringField("ip", UNKNOWN_IP, Store.YES));
                        doc.add(new StringField("pid", m.group(2), Store.YES));
                        indexWriter.addDocument(cache.getFacetsConfig().build(taxoWriter, doc));
                        records++;
                    }
                }
                logger.debug("Index built from " + pdfLog.getName() + " with " + records + " records parsed from " + lines + " lines.");
            } finally {
                reader.close();
                logger.info("Parsing " + pdfLog.getName() + " [completed]");
            }

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rwLog)));
            logger.info("Parsing " + rwLog.getName() + " [started]");
            Pattern rightsWrapperPattern = Pattern.compile("(\\d\\d\\d\\d-\\d\\d-\\d\\d).*\\Q - INFO  - Serviced request for \"\\E([^\"]+)\" \\(\\d+ bytes\\) in \\d+ms.");
            SimpleDateFormat RW_LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
            try {
                String line = null;
                int records = 0;
                int lines = 0;
                while ((line = reader.readLine()) != null) {
                    lines++;
                    Matcher m = rightsWrapperPattern.matcher(line);
                    if (m.matches()) {
                        Date date = RW_LOG_DATE_FORMAT.parse(m.group(1));
                        Document doc = new Document();
                        doc.add(new FacetField("action_facet", AccessType.RIGHTS_WRAPPER_DOWNLOAD.toString()));
                        doc.add(new StringField("action", AccessType.RIGHTS_WRAPPER_DOWNLOAD.toString(), Store.YES));
                        final String month = YEAR_MONTH.format(date);
                        doc.add(new StringField("month", month, Store.YES));
                        doc.add(new StringField("day", new SimpleDateFormat("dd").format(date), Store.YES));
                        doc.add(new StringField("ip", UNKNOWN_IP, Store.YES));
                        doc.add(new StringField("pid", m.group(2), Store.YES));
                        indexWriter.addDocument(cache.getFacetsConfig().build(taxoWriter, doc));
                        records++;
                    }
                }
                logger.debug("Index built from " + rwLog.getName() + " with " + records + " records parsed from " + lines + " lines.");
            } finally {
                reader.close();
                logger.info("Parsing " + rwLog.getName() + " [started]");
            }

            indexWriter.close();
            taxoWriter.close();
        }
    }

    public String getId() {
        return "farm logs";
    }

    public String getStartingAvailability() {
        return "2016-07";
    }

    public String getEndingAvailability() {
        return "2016-11";
    }

    public String toString() {
        return getId() + " (" + getStartingAvailability() + " to " + getEndingAvailability() + ")";
    }

}
