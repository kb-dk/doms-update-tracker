package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.Utils.asSet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class Utils {
    public static <T> Set<T> asSet(T... vars) {
        return new HashSet<T>(Arrays.asList(vars));
    }

    public static <T> Set<T> asSet(Class<T> type) {
        return new HashSet<T>();
    }

    static void addEntry(String pid, FedoraForUpdateTracker fcmock, String... contained) throws FedoraFailedException {
        when(fcmock.getEntryAngles(eq(pid), any(Date.class))).thenReturn(Utils.asSet(UpdateTrackerBackendTest.VIEW_ANGLE));
        when(fcmock.getState(eq(pid), any(Date.class))).thenReturn(Record.State.INACTIVE);
        List< String > objects = new ArrayList<String>(Arrays.asList(contained));
        objects.add(pid);
        when(fcmock.calcViewBundle(eq(pid), Matchers.eq(UpdateTrackerBackendTest.VIEW_ANGLE), any(Date.class))).thenReturn(new ViewBundle(pid,
                                                                                                                                          UpdateTrackerBackendTest.VIEW_ANGLE,
                                                                                                                                                  objects));
    }
}
