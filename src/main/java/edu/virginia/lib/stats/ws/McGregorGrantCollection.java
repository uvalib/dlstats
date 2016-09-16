package edu.virginia.lib.stats.ws;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import edu.virginia.lib.TracksysClient;

public class McGregorGrantCollection implements Collection {
    
    private Set<String> mcGregorPidsCached;
    
    private Set<String> getMcGregorPids() throws SQLException, IOException {
        if (mcGregorPidsCached != null) {
            return mcGregorPidsCached;
        } else {
            mcGregorPidsCached = new HashSet<String>();
            File tracksysProperties = new File("tracksys.properties");
            if (!tracksysProperties.exists()) {
                throw new RuntimeException("tracksys.properties is required to support the McGregor collection stats!");
            }
            Properties p = new Properties();
            FileInputStream fis = new FileInputStream(tracksysProperties);
            try {
                p.load(fis);
            } finally {
                fis.close();
            }
            final TracksysClient c = new TracksysClient(p.getProperty("url"), p.getProperty("username"), p.getProperty("password"));
            try {
                final int agencyId = c.lookupAgencyId("McGregor Grant");
                String sql = "select distinct pid from agencies join orders on agencies.id=orders.agency_id join units on units.order_id=orders.id join master_files on master_files.unit_id = units.id where agencies.id=?";
                PreparedStatement s = c.getDBConnection().prepareStatement(sql);
                s.setLong(1, agencyId);
                ResultSet rs = s.executeQuery();
                try {
                    while (rs.next()) {
                        mcGregorPidsCached.add(rs.getString(1));
                    }
                } finally {
                    rs.close();
                }
                
                sql = "select distinct pid from agencies join orders on agencies.id=orders.agency_id join units on units.order_id=orders.id join metadata on units.metadata_id=metadata.id where agencies.id=?";
                s = c.getDBConnection().prepareStatement(sql);
                s.setLong(1, agencyId);
                rs = s.executeQuery();
                try {
                    while (rs.next()) {
                        mcGregorPidsCached.add(rs.getString(1));
                    }
                } finally {
                    rs.close();
                }
                System.out.println("Identified " + mcGregorPidsCached.size() + " pids within the McGregor grant project.");
                return mcGregorPidsCached;
            } finally {
                c.closeConnection();
            }
        }
        
    }
    
    public String getName() {
        return "McGregor Grant";
    }
    
    public boolean isIdInCollection(String id) {
        try {
            return getMcGregorPids().contains(id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
