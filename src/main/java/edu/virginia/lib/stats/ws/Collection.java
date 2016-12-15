package edu.virginia.lib.stats.ws;

import java.io.IOException;

public interface Collection {

    public String getName();
    
    public boolean isIdInCollection(String id) throws IOException;

    /**
     * Sometimes collection membership is cached rather than determined on-demand.  In such cases
     * this method refreshes the cache to reflect the current collection membership.
     */
    public void reloadCollection();

    public static Collection EVERYTHING = new Collection() {

        public String getName() {
            return "Everything";
        }

        public boolean isIdInCollection(String id) {
            return true;
        }

        public void reloadCollection() {
            // does nothing
        }
    };


    
}
