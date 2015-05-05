package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentModelChangedLogging {
    private static Logger log = LoggerFactory.getLogger(ContentModelChangedLogging.class);

    public static void logContentModelChanged(Object pid){
        log.warn("Content model {} changed, but records are not recalculated", pid);
    }
}
