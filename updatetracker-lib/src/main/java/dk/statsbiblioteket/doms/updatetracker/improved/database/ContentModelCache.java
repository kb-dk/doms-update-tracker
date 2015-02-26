package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.util.caching.TimeSensitiveCache;

import java.util.Map;
import java.util.Set;

public class ContentModelCache {

    private static final int ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
    private static final TimeSensitiveCache<String, Set<String>> cache = new TimeSensitiveCache<>(ONE_DAY_IN_MILLISECONDS, true);


    public boolean isCachedContentModel(String pid){
        return cache.containsKey(pid);
    }

    public void invalidateContentModel(String pid){
        cache.remove(pid);
    }


    public Set<String> getCachedEntryAngles(String contentmodel) {
        return cache.get(contentmodel);
    }

    public void setEntryViewAngles(String contentmodel, Set<String> entryAngles) {
        cache.put(contentmodel,entryAngles);
    }

}
