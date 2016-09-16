package edu.virginia.lib.stats.ws;

import edu.virginia.lib.TracksysClient;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by md5wz on 10/28/16.
 */
public class SolrQueryCollectionTest {

    @Mock
    private TracksysClient mockTracksys;

    @Mock
    private SolrServer solr;

    @Mock
    private QueryResponse response1;

    @Mock
    private QueryResponse response2;

    @Mock
    private SolrDocumentList results1;
    @Mock
    private SolrDocumentList results2;

    @Mock
    private SolrDocument resultDoc;

    public static final String PID = "uva-lib:2137307";

    @Before
    public void setup() throws SolrServerException {
        initMocks(this);
        when(solr.query(any(SolrParams.class))).thenReturn(response1, response2);
        when(response1.getResults()).thenReturn(results1);
        when(response2.getResults()).thenReturn(results2);
        when(results1.size()).thenReturn(1);
        when(results1.get(0)).thenReturn(resultDoc);
        when(results2.size()).thenReturn(0);
        when(resultDoc.getFirstValue("id")).thenReturn(PID);
    }

    @Test
    public void testCreateNewIndex() throws Exception {
        List<String> pages = new ArrayList<>();
        pages.add("one");
        pages.add("two");
        when(mockTracksys.getPagePids(PID, false)).thenReturn(pages);

        SolrQueryCollection c = new SolrQueryCollection("First Test", "firstTest", "id:\"" + PID + "\"", mockTracksys, solr, "http://fake");
        try {
            assertTrue(PID + " should be in the index now!", c.isItemIndexed(new IndexSearcher(c.cache.getDirectoryReader()), PID));
            assertTrue(PID + " should be part of collection \"test\".", c.isIdInCollection(PID));
            assertFalse("fakepid should NOT be part of collection \"test\".", c.isIdInCollection("fakepid"));
            assertTrue("Page 'one' should be part of collection \"test\".", c.isIdInCollection("one"));
            assertTrue("Page 'two' should be part of collection \"test\".", c.isIdInCollection("two"));
            assertFalse("Page 'three' should NOT be part of collection \"test\".", c.isIdInCollection("three"));



        } finally {
            c.cache.deleteIndex();
        }
    }

}
