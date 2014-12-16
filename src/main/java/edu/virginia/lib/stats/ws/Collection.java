package edu.virginia.lib.stats.ws;

public interface Collection {

    public String getName();
    
    public boolean isIdInCollection(String id);
    
    public static Collection EVERYTHING = new Collection() {

        public String getName() {
            return "Everything";
        }

        public boolean isIdInCollection(String id) {
            return true;
        }};
    
}
