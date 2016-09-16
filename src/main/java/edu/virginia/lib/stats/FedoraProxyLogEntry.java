package edu.virginia.lib.stats;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.virginia.lib.stats.ws.DataSource.AccessType;

/**
 * Created by md5wz on 9/9/15.
 */
public class FedoraProxyLogEntry extends LogEntry {

    public FedoraProxyLogEntry(String line) {
        super(line);
    }

    public AccessType getType() {
        if (this.getStatusCode() == 0) {
            System.err.println("Unparsible log entry! " + raw);
            return AccessType.UNKNOWN;
        } else if (this.getStatusCode() < 200 || this.getStatusCode() > 299) {
            return AccessType.UNSUCCESSFUL;
        } else if (this.raw.contains("scriptTXT")) {
            return AccessType.SCRIPT_TEXT;
        } else if (raw.contains("scriptPDF")) {
            return AccessType.SCRIPT_PDF;
        } else if (raw.contains("getThumbnail") || raw.contains("datastreams/thumbnail/content")) {
            return AccessType.THUMBNAIL;
        } else if (raw.contains("uva-lib:2141110")) {
            return AccessType.POLICY;
        } else if (raw.contains("getScaled")) {
            return AccessType.IMAGE_DOWNLOAD;
        } else if (raw.contains("getStaticImage")) {
            return AccessType.RIGHTS_WRAPPER_DOWNLOAD;
        } else if (raw.contains("getRegion")) {
            return AccessType.REGION;
        } else if (raw.contains("getMetadata")) {
            return AccessType.REGION;
        } else if (raw.contains("getMODS") || raw.contains("getMETS")) {
            return AccessType.DPLA_HARVEST;
        } else if (raw.contains("/fedora/")) {
            return AccessType.DIRECT_FEDORA_ACCESS;
        } else {
            return AccessType.UNKNOWN;
        }
    }

    public String getPid() {
        Pattern p = Pattern.compile(".*/fedora/.*/(uva-lib:\\d+)[ /]+.*");
        Matcher m = p.matcher(raw);
        if (m.matches()) {
            return m.group(1);
        } else {
            Pattern p2 = Pattern.compile(".*/fedora/.*/(uva-lib%3a\\d+)[ /]+.*");
            Matcher m2 = p2.matcher(raw.toLowerCase());
            if (m2.matches()) {
                return m2.group(1).replace("%3a", ":");
            } else {
                return null;
            }
        }
    }
}
