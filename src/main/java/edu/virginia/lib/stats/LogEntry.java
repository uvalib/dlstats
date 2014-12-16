package edu.virginia.lib.stats;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A java class that encapsulates and represents a single line from an apache log file.
 * The log is expected to be in the combined log format containing a referrer and user
 * agent.
 *
 * Accessor methods exist in this class expose parsed or transformed information from
 * that log entry.  By convention these methods should not return null when possible
 * and should favor a display-ready value such as the empty String or empty Collections.
 */
public class LogEntry {

    private static final Pattern LOG_PATTERN = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\Q - - \\E\\[([^\\]]+)\\] \"([^ ]+) ([^ ]+) .+\" (\\d+) (\\d+|-)( \"(.+)\" \"(.+)\")?");
    private static final Pattern ALT_LOG_PATTERN = Pattern.compile("(::1)\\Q - - \\E\\[([^\\]]+)\\] \"([^ ]+) ([^ ]+) .+\" (\\d+) (\\d+|-)( \"(.+)\" \"(.+)\")?");


    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z");

    protected String raw;

    private Matcher m;

    public LogEntry(final String line) {
        raw = line;
        m = LOG_PATTERN.matcher(line);
        if (!m.matches()) {
            m = ALT_LOG_PATTERN.matcher(line);
            if (!m.matches()) {
                throw new RuntimeException("Unparsible line: " + line);
            }
        }
        if (getStatusCode() == 0 || getDate() == null) {
            throw new RuntimeException();
        }
    }

    public Date getDate() {
        try {
            return m.matches() ? DATE_FORMAT.parse(m.group(2)) : null;
        } catch (ParseException e) {
            return null;
        }
    }

    public String getDateString() {
        return m.matches() ? m.group(2) : null;
    }

    public String getClientIPAddress() {
        return m.matches() ? m.group(1) : "";
    }

    public String getHttpRequestMethod() {
        return m.matches() ? m.group(3) : "";
    }

    public String getRequestedResourcePath() {
        return m.matches() ? m.group(4) : "";
    }

    public int getStatusCode() { return m.matches() ? Integer.parseInt(m.group(5)) : 0; }

    public String getReferrer() {
        return m.matches() ? m.group(8) : "";
    }

    public String getUserAgent() {
        return m.matches() ? m.group(9) : "";
    }

    /**
     * A helper method that may be invoked to report some non-fatal condition that
     * may degrade the quality of information available but is not outside the realm
     * of expected possibilities.
     *
     * This message may be useful in improving or debugging the analysis routines
     * but need not be reflected in any resulting reports.
     *
     * @param message a message explaining the situation
     */
    protected void reportNonFatalIssue(String message) {
        System.err.println(message + " (" + raw + ")");
    }
}
