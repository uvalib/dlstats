package edu.virginia.lib.stats;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by md5wz on 9/9/15.
 */
public class FedoraProxyLogEntry extends LogEntry {

    public enum AccessType {
        SCRIPT_TEXT,
        SCRIPT_PDF,
        THUMBNAIL,
        POLICY,
        IMAGE_DOWNLOAD,
        RIGHTS_WRAPPER_DOWNLOAD,
        REGION,
        DIRECT_FEDORA_ACCESS,
        UNSUCCESSFUL,
        DPLA_HARVEST,
        UNKNOWN;
        
        public static AccessType fromString(String string) {
            for (AccessType t : values()) {
                if (string.equals(t.toString())) {
                    return t;
                }
            }
            return null;
        }
        
        public String toString() {
            switch (this) {
            case SCRIPT_TEXT:
                return "Anchor Script Text";
            case SCRIPT_PDF:
                return "Anchor Script PDF";
            case THUMBNAIL:
                return "Thumbnail";
            case POLICY:
                return "Policy request";
            case IMAGE_DOWNLOAD:
                return "Static image download";
            case RIGHTS_WRAPPER_DOWNLOAD:
                return "Rights-wrapper download";
            case REGION:
                return "Region download (interactive view)";
            case DIRECT_FEDORA_ACCESS:
                return "Unclassified Fedora access (likely internal staff)";
            case DPLA_HARVEST:
                return "DPLA Harvest";
            case UNSUCCESSFUL:
                return "Unsuccessful request";
            case UNKNOWN:
                return "Unknown access";
            default:
                return "Unknown";
            }
        }
    }

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
