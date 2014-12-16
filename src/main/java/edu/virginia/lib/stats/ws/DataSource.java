package edu.virginia.lib.stats.ws;

import java.util.Set;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import edu.virginia.lib.stats.HitMap;

public interface DataSource {
    
    public String getId();
    
    /**
     * Inclusive, formatted as 2016-01.
     */
    public String getStartingAvailability();
    
    /**
     * Inclusive, formatted as 2016-01.
     */
    public String getEndingAvailability();
    
    /**
     * Iterates over all known accesses for the given month, filtering to just 
     * include the given collection and marks hits for each action performed to
     * the action HitMap and each item access on the item HitMap.
     */
    public void getActionCountsPerMonth(String month, Collection c, HitMap actions, boolean excludeKnownBots);
    
    public void getPopularItemCountsForMonth(String month, Collection c, Set<FedoraProxyLogEntry.AccessType> actions, HitMap items, boolean excludeKnownBots);
}
