package edu.virginia.lib.stats.ws;

import java.util.Set;

import edu.virginia.lib.stats.FedoraProxyLogEntry;
import edu.virginia.lib.stats.HitMap;

public interface DataSource {
    
    public static enum AccessType {
        SCRIPT_TEXT("WSLS Anchor Script Text", "The number of times a WSLS anchor script text was downloaded.", false),
        SCRIPT_PDF("WSLS Anchor Script PDF", "The number of times a WSLS anchor script PDF was downloaded.", false),
        THUMBNAIL("Thumbnail", "The number of times a thumbnail was presented to a user.  These thumbnails are shown in search results and on the details page in Virgo.", false),
        POLICY("Policy request", "", true),
        IMAGE_DOWNLOAD("Image Download", "The number of times a lower resolution version of the image was downloaded.", true),
        RIGHTS_WRAPPER_DOWNLOAD("Full-sized Image download", "The number of times a full-sized image was downloaded.  This image includes information including a citation and text about the terms of use.", false),
        REGION("Image region request", "", false),
        PAGE_PREVIEW("Page preview", "", true),
        DIRECT_FEDORA_ACCESS("Direct fedora access", "", true),
        UNSUCCESSFUL("Unsuccessful request", "", true),
        DPLA_HARVEST("DPLA Harvest", "", true),
        METADATA("IIIF Metadata", "", true),
        INTERNAL("Internal", "", true),
        PDF_DOWNLOAD("PDF Download", "The number of times a PDF was downloaded.  This is the PDF that includes all pages of a multi-page item (like a book).", false),
        TRACKSYS_PREVIEW("Tracksys Preview", "", true),
        SESSIONS("Interactive View Sessions", "The number of times a user viewed the interactive image viewer for the given page at least once in a day.", false),
        UNKNOWN("Unknown", "", true);

        private String label;
        private boolean suppress;
        private String description;

        AccessType(final String label, final String description, final boolean suppress) {
            this.label = label;
            this.description = description;
            this.suppress = suppress;
        }

        public static AccessType fromString(String string) {
            for (AccessType t : values()) {
                if (string.equals(t.toString())) {
                    return t;
                }
            }
            return null;
        }
        
        public String toString() {
            return this.label;
        }

        public String getDescription() {
            return this.description;
        }

        public boolean shouldBeSuppressed() {
            return this.suppress;
        }
    }
    
    public String getId();
    
    /**
     * Inclusive, formatted as 2016-01.
     */
    public String getStartingAvailability();
    
    /**
     * Inclusive, formatted as 2016-01.
     */
    public String getEndingAvailability();
    
    /**
     * Iterates over all known accesses for the given month, filtering to just 
     * include the given collection and marks hits for each action performed to
     * the action HitMap and each item access on the item HitMap.
     */
    public void getActionCountsPerMonth(String month, Collection c, HitMap actions, boolean excludeKnownBots);
    
    public void getPopularItemCountsForMonth(String month, Collection c, Set<AccessType> actions, HitMap items, boolean excludeKnownBots);
}
