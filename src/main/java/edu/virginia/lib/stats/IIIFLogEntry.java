package edu.virginia.lib.stats;

import edu.virginia.lib.stats.ws.DataSource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by md5wz on 10/26/16.
 */
public class IIIFLogEntry extends LogEntry {

    public IIIFLogEntry(String line) {
        super(line);
    }

    public DataSource.AccessType getType() {
        if (this.getStatusCode() == 0) {
            System.err.println("Unparsible log entry! " + raw);
            return DataSource.AccessType.UNKNOWN;
        } else if (this.getStatusCode() < 200 || this.getStatusCode() > 299) {
            return DataSource.AccessType.UNSUCCESSFUL;
        } else if (raw.contains("!125,125")) {
            return DataSource.AccessType.THUMBNAIL;
        } else if (raw.contains("!100,100")) {
            return DataSource.AccessType.PAGE_PREVIEW;
        } else if (raw.contains("full")) {
            if (raw.contains("pct:")) {
                return DataSource.AccessType.INTERNAL;
            }
            if (raw.contains("/,640")) {
                return DataSource.AccessType.TRACKSYS_PREVIEW;
            }
            Pattern p = Pattern.compile(".*/full/!(\\d+),(\\d+)/.*");
            Matcher m = p.matcher(raw);
            if (m.matches()) {
                if (Integer.parseInt(m.group(1)) > 200 && Integer.parseInt(m.group(2)) > 200) {
                    return DataSource.AccessType.IMAGE_DOWNLOAD;
                } else {
                    return DataSource.AccessType.THUMBNAIL;
                }
            } else {
                return DataSource.AccessType.UNKNOWN;
            }
        } else if (raw.contains("info.json")) {
            return DataSource.AccessType.METADATA;
        } else if (raw.contains("default.jpg")) {
            return DataSource.AccessType.REGION;
        } else {
            return DataSource.AccessType.UNKNOWN;
        }
    }

    public String getPid() {
        Pattern p = Pattern.compile(".*/iiif/([^/]*)/.*");
        Matcher m = p.matcher(raw);
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }
}
