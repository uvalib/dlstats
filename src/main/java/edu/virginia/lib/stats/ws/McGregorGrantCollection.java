package edu.virginia.lib.stats.ws;

import edu.virginia.lib.TracksysClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class McGregorGrantCollection implements Collection {

    final Logger logger = LoggerFactory.getLogger(McGregorGrantCollection.class);

    private Set<String> mcGregorPidsCached;

    public McGregorGrantCollection(final TracksysClient c) throws SQLException, IOException {
        mcGregorPidsCached = new HashSet<>();
        logger.info("Looking up McGregor grant pids [started]");
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
            logger.debug("Identified " + mcGregorPidsCached.size() + " pids within the McGregor grant project.");
        } finally {
            logger.info("Looking up McGregor grant pids [completed]");
        }
    }
    
    public String getName() {
        return "McGregor Grant";
    }
    
    public boolean isIdInCollection(String id) {
        return this.mcGregorPidsCached.contains(id);
    }

}
