package edu.virginia.lib.stats.ws;

import java.io.IOException;

public interface Collection {

    public String getName();
    
    public boolean isIdInCollection(String id) throws IOException;
    
    public static Collection EVERYTHING = new Collection() {

        public String getName() {
            return "Everything";
        }

        public boolean isIdInCollection(String id) {
            return true;
        }};
    
}
