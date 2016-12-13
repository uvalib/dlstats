package edu.virginia.lib.stats.ws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import edu.virginia.lib.TracksysClient;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import edu.virginia.lib.stats.HitMap;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;

@Path("/")
@Singleton
public class Collections implements ContainerLifecycleListener {

    private static final String VERSION = "1.0";
    
    private Collection[] collections;

    private TracksysClient tracksys;

    /**
     * It's currently assumed that any data source applies to all collections.
     */
    private DataSource[] dataSources;
    
    public Collections() throws Exception {
        Properties config = new Properties();
        FileInputStream configFis = new FileInputStream("config.properties");
        try {
            config.load(configFis);
        } finally {
            configFis.close();
        }

        final SolrServer solr = new HttpSolrServer(config.getProperty("solr.url"));
        ((HttpSolrServer) solr).setParser(new XMLResponseParser());

        tracksys = new TracksysClient(config.getProperty("tracksys.url"), config.getProperty("tracksys.username"), config.getProperty("tracksys.password"));
        collections = new Collection[]{
                Collection.EVERYTHING,
                new McGregorGrantCollection(tracksys),
                new PidListCollection(Collections.class.getClassLoader().getResourceAsStream("daily-progress-pids.txt"), "Daily Progress"),
                new SolrQueryCollection("Law Library", "law", "+source_facet:\"UVA Library Digital Repository\" +library_facet:\"Law School\" -shadowed_location_facet:\"HIDDEN\"", tracksys, solr, config.getProperty("fedora.url")),
                new SolrQueryCollection("Health Sciences Library", "health_sciences", "+source_facet:\"UVA Library Digital Repository\" +library_facet:\"Health Sciences\" -shadowed_location_facet:\"HIDDEN\"", tracksys, solr, config.getProperty("fedora.url")),
                new SolrQueryCollection("Special Collections", "special_collections", "+source_facet:\"UVA Library Digital Repository\" +library_facet:\"Special Collections\" -shadowed_location_facet:\"HIDDEN\"", tracksys, solr, config.getProperty("fedora.url"))};
        dataSources = new DataSource[]{
                new FedoraProxyLogDataSource(new File("2016-11/fedoraproxy-all.log")),
                new MicroServiceLogDataSource(new File("2016-11/iiif-http-all.log"), new File("2016-11/pdf-all.log"), new File("2016-11/rightswrapper2-all.log"))};
    }

    @GET
    @Path("stats.js")
    @Produces("text/javascript")
    public Response getJavascript() {
        return Response.ok().encoding("UTF-8").entity(this.getClass().getClassLoader().getResourceAsStream("stats.js")).build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response getSimpleFormHTML(@Context UriInfo uriInfo) {
        StringBuffer response = new StringBuffer();
        response.append("<html>\n" + 
                "  <head>\n" + 
                "    <title>DL Stats</title>\n" +
                "  </head>\n" +
                "  <body>\n" +
                "    <script src=\"" + uriInfo.getRequestUri().toString() + (uriInfo.getRequestUri().toString().endsWith("/") ? "" : "/") + "stats.js\" type=\"text/javascript\" ></script>\n" +
                "  <h2>Request Activity Summary</h2>\n" + 
                "  <form method=\"GET\" action=\"" + uriInfo.getBaseUriBuilder().path("summary").build().toString() + "\">\n" +
                "    <fieldset>\n" +
                "      <legend>Select Collection:</legend>\n" + 
                "      <select id=\"summary-collections\" name=\"collectionId\">\n"); 
        for (int i = 0; i < collections.length; i ++) {
            Collection c = collections[i];
            response.append("       <option value=\"" + i + "\">" + c.getName() + "</option>\n");
        }
        response.append("      </select>\n" +
                "      <label><input type=\"checkbox\" name=\"excludeKnownBots\" value=\"true\" checked />Exclude activity from known bots and crawlers.</label>\n" +
                "    </fieldset>\n" +
                "    <fieldset id=\"summary-months\">\n" + 
                "      <legend>Select Months to Include:</legend>\n");
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2014);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        Calendar endMonth = Calendar.getInstance();
        endMonth.setTime(new Date());
        endMonth.add(Calendar.YEAR, 1);
        while(c.get(Calendar.YEAR) < endMonth.get(Calendar.YEAR)) {
            if (c.get(Calendar.MONTH) == Calendar.JANUARY) {
                response.append("      <div class=\"year\" id=\"year-" + c.get(Calendar.YEAR) + "\">\n");
            }
            final String currentMonth = (new SimpleDateFormat("yyyy-MM")).format(c.getTime());
            boolean available = false;
            for (int i = 0; i < dataSources.length; i ++) {
                final DataSource ds = dataSources[i];
                if (ds.getStartingAvailability().compareTo(currentMonth) <= 0 && ds.getEndingAvailability().compareTo(currentMonth) >= 0) {
                    available = true;
                }
            }
            response.append("        <label><input type=\"checkbox\" name=\"months\" value=\"" + currentMonth + "\" " + (available ? "" : "disabled") + "/>" + currentMonth + "</label>\n");
            if (c.get(Calendar.MONTH) == Calendar.DECEMBER) {
                response.append("      </div>\n");
            }
            c.add(Calendar.MONTH, 1);
        }
        response.append("    </fieldset>\n" +
                "    <p>Generation of these reports may take several minutes.  Report generation time will increase in proportion to the number of months selected or if filtering a particular collection.</p>\n" +
                "    <input type=\"submit\" value=\"Generate Report\" />\n" +
                "    <input type=\"reset\" value=\"Reset Form\" />\n" +
                "  </form>\n" + 
                "</html>");
        return Response.ok().encoding("UTF-8").entity(response.toString()).build();
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("collections")
    public JsonArray listCollections() {
        JsonArrayBuilder a = Json.createArrayBuilder();
        for (int i = 0; i < collections.length; i ++) {
            a.add(Json.createObjectBuilder().add("id", i).add("label", collections[i].getName()).build());
        }
        return a.build();
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("accessTypes")
    public JsonArray listAccessTypes() {
        JsonArrayBuilder a = Json.createArrayBuilder();
        for (DataSource.AccessType t : DataSource.AccessType.values()) {
            a.add(Json.createObjectBuilder().add("id", t.name()).add("label", t.toString()).build());
        }
        return a.build();
    }
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("months")
    public JsonArray listAvailableMonths() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2014);
        c.set(Calendar.MONTH, Calendar.NOVEMBER);
        
        JsonArrayBuilder a = Json.createArrayBuilder();
        while(c.getTime().before(new Date())) {
            final String currentMonth = (new SimpleDateFormat("yyyy-MM")).format(c.getTime());
            for (int i = 0; i < dataSources.length; i ++) {
                final DataSource ds = dataSources[i];
                if (ds.getStartingAvailability().compareTo(currentMonth) <= 0 && ds.getEndingAvailability().compareTo(currentMonth) >= 0) {
                    a.add(currentMonth);
                }
            }
            c.add(Calendar.MONTH, 1);
        }
        return a.build();
    }
    
    @GET
    @Produces("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    @Path("summary")    
    public Response getSummaryReport(@QueryParam("collectionId") final int collectionId, @QueryParam("months") final List<String> months, @QueryParam("excludeKnownBots") boolean excludeKnownBots) throws SQLException {
        final Collection c = collections[collectionId];
        XSSFWorkbook wb = new XSSFWorkbook();
        wb.getProperties().getCustomProperties().addProperty("Stats App Version", VERSION);
        wb.getProperties().getCustomProperties().addProperty("Excludes Known Bots", String.valueOf(excludeKnownBots));
        addSummary(wb, c, months, excludeKnownBots);
        addPopularityReport(wb, c, months, java.util.Collections.singleton(DataSource.AccessType.RIGHTS_WRAPPER_DOWNLOAD.name()), excludeKnownBots, 100);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            wb.write(baos);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return Response.ok().type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet").entity(new ByteArrayInputStream(baos.toByteArray())).build();
    }
    
    @GET
    @Produces("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    @Path("popularity")    
    public Response getPopularityReport(@QueryParam("collectionId") final int collectionId, @QueryParam("months") final List<String> months, @QueryParam("actions") final List<String> actions, @QueryParam("excludeKnownBots") boolean excludeKnownBots) throws SQLException {
        final Collection c = collections[collectionId];
        XSSFWorkbook wb = new XSSFWorkbook();
        wb.getProperties().getCustomProperties().addProperty("Stats App Version", VERSION);
        wb.getProperties().getCustomProperties().addProperty("Excludes Known Bots", String.valueOf(excludeKnownBots));
        addPopularityReport(wb, c, months, actions, excludeKnownBots, 100);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            wb.write(baos);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return Response.ok().type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet").entity(new ByteArrayInputStream(baos.toByteArray())).build();
    }
    
    private void addSummary(Workbook wb, Collection c, List<String> months, boolean excludeKnownBots) {
        HitMap totalActions = new HitMap();
        Map<String, HitMap> monthHitMaps = new HashMap<String, HitMap>();
        for (String month : months) {
            HitMap actions = new HitMap();
            for (DataSource d : dataSources) {
                d.getActionCountsPerMonth(month, c, actions, excludeKnownBots);
            }
            totalActions.addAll(actions);
            monthHitMaps.put(month, actions);
        }
        
        List<String> actionTypes = new ArrayList<String>(totalActions.getKeys());
        for (DataSource.AccessType type : DataSource.AccessType.values()) {
            if (type.shouldBeSuppressed()) {
                actionTypes.remove(type.toString());
            }
        }
        java.util.Collections.sort(actionTypes);
        Sheet s = wb.createSheet("Access summary for " + c.getName());
        Row header = s.createRow(0);
        addCell(header, 0, "year-month");
        for (int i = 0; i < actionTypes.size(); i ++) {
            addCell(header, i + 1, actionTypes.get(i).toString());
        }
        Row description = s.createRow(1);
        for (int i = 0; i < actionTypes.size(); i ++) {
            for (DataSource.AccessType type : DataSource.AccessType.values()) {
                if (type.toString().equals(actionTypes.get(i))) {
                    addCell(description, i + 1, type.getDescription());
                }
            }
        }
        for (int i = 0; i < months.size(); i ++) {
            String month = months.get(i);
            HitMap monthHitMap = monthHitMaps.get(month);
            Row r = s.createRow(i + 2);
            addCell(r, 0, month);
            for (int j = 0; j < actionTypes.size(); j ++) {
                addCell(r, j + 1, String.valueOf(monthHitMap.getHits(actionTypes.get(j).toString())));
            }
        }
    }
    
    /**
     * Adds a Sheet to the given Workbook that lists the top items for the
     * given accesses over all the months included.
     */
    private void addPopularityReport(Workbook wb, Collection c, List<String> months, final java.util.Collection<String> actions, boolean excludeKnownBots, int itemCount) throws SQLException {
        final HitMap items = new HitMap();
        Set<DataSource.AccessType> actionTypes = new HashSet<DataSource.AccessType>();
        for (String action : actions) {
            actionTypes.add(DataSource.AccessType.valueOf(action));
        }
        for (String month : months) {
            for (DataSource d : dataSources) {
                d.getPopularItemCountsForMonth(month, c, actionTypes, items, excludeKnownBots);
            }
        }
        

        Sheet s = wb.createSheet("Top " + itemCount + " items in " + c.getName());
        Row header = s.createRow(0);
        addCell(header, 0, "Image PID");
        addCell(header, 1, summarizeAccessTypes(actionTypes));
        addCell(header, 2, "description");
        addCell(header, 3, "URL");

        ArrayList<String> sorted = new ArrayList<String>();
        for (String pid : items.getKeys()) {
            sorted.add(pid);
        }
        java.util.Collections.sort(sorted, new Comparator<String>() {
            @Override
            public int compare(String first, String second) {
                return items.getHits(second) - items.getHits(first);
            }});

        for (int i = 0; i < sorted.size() && i < itemCount; i ++) {
            Row r = s.createRow(i + 1);
            addCell(r, 0, sorted.get(i));
            addCell(r, 1, String.valueOf(items.getHits(sorted.get(i))));
            TracksysClient.Summary summary = tracksys.getDescriptionOfPid(sorted.get(i));
            addCell(r, 2, summary != null ? summary.getTitle() : "");
            addCell(r, 3, summary != null ? summary.getUrl() : "");
        }
    }
    
    private String summarizeAccessTypes(java.util.Collection<DataSource.AccessType> actions) {
        StringBuffer sb = new StringBuffer();
        for (DataSource.AccessType action : actions) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(action);
        }
        return "Number of " + sb.toString();
    }
    
    public void addCell(Row r, int index, String value) {
        Cell c = r.createCell(index);
        c.setCellValue(value);
    }

    @Override
    public void onStartup(Container container) {

    }

    @Override
    public void onReload(Container container) {

    }

    @Override
    public void onShutdown(Container container) {
        try {
            tracksys.closeConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
