package dk.statsbiblioteket.doms.updatetracker.improved;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Utils {
    public static <T> Set<T> asSet(T... vars) {
        return new HashSet<T>(Arrays.asList(vars));
    }

    public static <T> Set<T> asSet(Class<T> type) {
        return new HashSet<T>();
    }
}
