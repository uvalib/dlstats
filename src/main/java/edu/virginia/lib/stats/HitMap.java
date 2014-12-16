package edu.virginia.lib.stats;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HitMap {
    private Map<String, Integer> hits = new HashMap<String, Integer>();

    public void hit(String key) {
        hit(key, 1);
    }
    
    public void hit(String key, int magnitude) {
        Integer i = hits.get(key);
        if (i == null) {
            hits.put(key, magnitude);
        } else {
            hits.put(key, i.intValue() + magnitude);
        }
    }
    
    public void addAll(HitMap other) {
        for (Map.Entry<String, Integer> entry : other.hits.entrySet()) {
            hit(entry.getKey(), entry.getValue());
        }
    }

    public Set<String> getKeys() {
        return hits.keySet();
    }
    
    public int getHits(String key) {
        try {
            return hits.get(key);
        } catch (NullPointerException e) {
            return 0;
        }
    }
    
    public void printOut(PrintWriter p) {
        ArrayList<String> keys = new ArrayList<String>(hits.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            p.println("\"" + key + "\"," +  hits.get(key));
        }
    }

    public void printAsCSV(PrintWriter p) {
        ArrayList<String> keys = new ArrayList<String>(hits.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            p.println(key + "," +  hits.get(key));
        }
    }
}
