package edu.virginia.lib.stats.ws;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfirmedBotClientFilter implements ClientFilter {
    
    public boolean include(String ip) {
        Matcher m = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)").matcher(ip);
        if (m.matches()) {
            int first  = Integer.parseInt(m.group(1));
            int second  = Integer.parseInt(m.group(2));
            int third  = Integer.parseInt(m.group(3));
            int fourth  = Integer.parseInt(m.group(4));
            /* Google bots 
               64.233.160.0    64.233.191.255
               66.102.0.0  66.102.15.255
               66.249.64.0     66.249.95.255
               72.14.192.0     72.14.255.255
               74.125.0.0  74.125.255.255
               209.85.128.0    209.85.255.255
               216.239.32.0    216.239.63.255
            */

            if (first == 64 && second == 233) {
                return false;
            }
            if (first == 66 && second == 102) {
                return false;
            }
            if (first == 66 && second == 249) {
                return false;
            }
            if (first == 72 && second == 14) {
                return false;
            }
            if (first == 74 && second == 125) {
                return false;
            }
            if (first == 209 && second == 85) {
                return false;
            }
            if (first == 216 && second == 239) {
                return false;
            }
            /* MSN 
               64.4.0.0    64.4.63.255
               65.52.0.0   65.55.255.255
               131.253.21.0    131.253.47.255
               157.54.0.0  157.60.255.255
               207.46.0.0  207.46.255.255
               207.68.128.0    207.68.207.255
             */
            if (first == 64 && second == 4) {
                return false;
            }
            if (first == 65 && second == 52) {
                return false;
            }
            if (first == 131 && second == 253) {
                return false;
            }
            if (first == 157 && second == 54) {
                return false;
            }
            if (first == 207 && second == 46) {
                return false;
            }
            if (first == 207 && second == 68) {
                return false;
            }
            
            /* YAHOO
               8.12.144.0     8.12.144.255S
               66.196.64.0     66.196.127.255
               66.228.160.0    66.228.191.255
               67.195.0.0  67.195.255.255
               68.142.192.0    68.142.255.255
               72.30.0.0   72.30.255.255
               74.6.0.0    74.6.255.255
               98.136.0.0  98.139.255.255
               202.160.176.0   202.160.191.255
               209.191.64.0    209.191.127.255
             */
            if (first == 8 && second == 12) {
                return false;
            }
            if (first == 66 && second == 196) {
                return false;
            }
            if (first == 66 && second == 228) {
                return false;
            }
            if (first == 67 && second == 195) {
                return false;
            }
            if (first == 68 && second == 142) {
                return false;
            }
            if (first == 72 && second == 30) {
                return false;
            }
            if (first == 74 && second == 6) {
                return false;
            }
            if (first == 98 && second == 136) {
                return false;
            }
            if (first == 202 && second == 160) {
                return false;
            }
            if (first == 209 && second == 191) {
                return false;
            }       
        }
        return true;
    }

}
