package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import dk.statsbiblioteket.util.caching.TimeSensitiveCache;

import java.util.Map;
import java.util.Set;

public class EntryAngleCache {

    private static final int ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    private static final TimeSensitiveCache<String, Set<String>> cache = new TimeSensitiveCache<>(ONE_DAY_IN_MILLISECONDS, true);


    public synchronized boolean isCachedContentModel(String pid){
        return cache.containsKey(pid);
    }

    public synchronized void invalidateContentModel(String pid){
        cache.remove(pid);
    }


    public synchronized Set<String> getCachedEntryAngles(String contentmodel) {
        return cache.get(contentmodel);
    }

    public synchronized void setEntryViewAngles(String contentmodel, Set<String> entryAngles) {
        cache.put(contentmodel,entryAngles);
    }

}
