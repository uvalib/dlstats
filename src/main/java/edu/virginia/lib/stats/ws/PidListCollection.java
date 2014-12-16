package edu.virginia.lib.stats.ws;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class PidListCollection implements Collection {

    private Set<String> pids;
    
    private String name;
    
    public PidListCollection(InputStream is, final String name) throws IOException {
        this.name = name;
        this.pids = new HashSet<String>();
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = r.readLine()) != null) {
            pids.add(line.trim());
        }
        r.close();
        
    }
    
    public String getName() {
        return name;
    }

    public boolean isIdInCollection(String id) {
        return pids.contains(id);
    }

}
