package edu.virginia.lib.stats.ws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import edu.virginia.lib.stats.HitMap;

@Path("/")
public class Collections {

    private static final String VERSION = "1.0";
    
    private Collection[] collections;
    
    /**
     * It's currently assumed that any data source applies to all collections.
     */
    private DataSource[] dataSources;
    
    public Collections() throws IOException {
        collections = new Collection[] { Collection.EVERYTHING, new McGregorGrantCollection(), new PidListCollection(Collections.class.getClassLoader().getResourceAsStream("daily-progress-pids.txt"), "Daily Progress") };
        dataSources = new DataSource[] { new FedoraProxyLogDataSource(new File("fedoraproxy-all.log")) };
    }
    
    @GET
    @Produces(MediaType.TEXT_HTML)
    public String getSimpleFormHTML() {
        StringBuffer response = new StringBuffer();
        response.append("<html>\n" + 
                "  <head>\n" + 
                "    <title>DL Stats</title>\n" + 
                "  </head>\n" + 
                "  <body>\n" + 
                "  <h2>Request Activity Summary</h2>\n" + 
                "  <form method=\"GET\" action=\"summary\">\n" +
                "    <fieldset>\n" +
                "      <legend>Select Collection:</legend>\n" + 
                "      <select id=\"summary-collections\" name=\"collectionId\">\n"); 
        for (int i = 0; i < collections.length; i ++) {
            Collection c = collections[i];
            response.append("       <option value=\"" + i + "\">" + c.getName() + "</option>\n");
        }
        response.append("      </select>\n" +
                "      <label><input type=\"checkbox\" name=\"excludeKnownBots\" value=\"true\" />Exclude activity from known bots and crawlers.</label>\n" +
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
                response.append("      <div class=\"year\">\n");
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
                "    <input type=\"submit\" value=\"Generate Report\" />\n" + 
                "  </form>\n" + 
                "</html>");
        return response.toString();
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
        for (FedoraProxyLogEntry.AccessType t : FedoraProxyLogEntry.AccessType.values()) {
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
    public Response getSummaryReport(@QueryParam("collectionId") final int collectionId, @QueryParam("months") final List<String> months, @QueryParam("excludeKnownBots") boolean excludeKnownBots) {
        final Collection c = collections[collectionId];
        XSSFWorkbook wb = new XSSFWorkbook();
        wb.getProperties().getCustomProperties().addProperty("Stats App Version", VERSION);
        wb.getProperties().getCustomProperties().addProperty("Excludes Known Bots", String.valueOf(excludeKnownBots));
        addSummary(wb, c, months, excludeKnownBots);
        addPopularityReport(wb, c, months, java.util.Collections.singleton(FedoraProxyLogEntry.AccessType.RIGHTS_WRAPPER_DOWNLOAD.name()), excludeKnownBots, 100);
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
    public Response getPopularityReport(@QueryParam("collectionId") final int collectionId, @QueryParam("months") final List<String> months, @QueryParam("actions") final List<String> actions, @QueryParam("excludeKnownBots") boolean excludeKnownBots) {
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
        java.util.Collections.sort(actionTypes);
        Sheet s = wb.createSheet("Access summary for " + c.getName());
        Row header = s.createRow(0);
        addCell(header, 0, "year-month");
        for (int i = 0; i < actionTypes.size(); i ++) {
            addCell(header, i + 1, actionTypes.get(i).toString());
        }
        for (int i = 0; i < months.size(); i ++) {
            String month = months.get(i);
            HitMap monthHitMap = monthHitMaps.get(month);
            Row r = s.createRow(i + 1);
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
    private void addPopularityReport(Workbook wb, Collection c, List<String> months, final java.util.Collection<String> actions, boolean excludeKnownBots, int itemCount) {
        final HitMap items = new HitMap();
        Set<FedoraProxyLogEntry.AccessType> actionTypes = new HashSet<FedoraProxyLogEntry.AccessType>();
        for (String action : actions) {
            actionTypes.add(FedoraProxyLogEntry.AccessType.valueOf(action));
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
        }
    }
    
    private String summarizeAccessTypes(java.util.Collection<FedoraProxyLogEntry.AccessType> actions) {
        StringBuffer sb = new StringBuffer();
        for (FedoraProxyLogEntry.AccessType action : actions) {
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
    
}
