package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

/**
 * This timertasks regularly polls the fedora worklog and calls the update tracker with the unit of work
 */
public class WorkLogPollTask extends TimerTask {

    private final WorkLogPoller workLogPoller;
    private final UpdateTrackerPersistentStore updateTrackerPersistentStore;
    private int limit;

    /**
     *
     * @param workLogPoller
     * @param updateTrackerPersistentStore
     * @param limit the amount of work units to retrieve in each invocation
     */
    public WorkLogPollTask(WorkLogPoller workLogPoller,
                           UpdateTrackerPersistentStore updateTrackerPersistentStore, int limit) {
        this.workLogPoller = workLogPoller;
        this.updateTrackerPersistentStore = updateTrackerPersistentStore;
        this.limit = limit;
    }

    @Override
    public void run() {
        try {
            //TODO DO NOT USE LASTMODIFIED, USE THE INCREMENTING KEY
            Date lastRegisteredChange = updateTrackerPersistentStore.lastChanged();
            List<WorkLogUnit> events = workLogPoller.getFedoraEvents(lastRegisteredChange, limit);

            for (WorkLogUnit event : events) {
                String pid = event.getPid();
                Date date = event.getDate();
                String param = event.getParam();

                switch (event.getMethod()) {
                    case "ingest":
                        updateTrackerPersistentStore.objectCreated(pid, date);
                        break;
                    case "modifyObject":
                        updateTrackerPersistentStore.objectStateChanged(pid, date, param);
                        break;
                    case "purgeObject":
                        updateTrackerPersistentStore.objectDeleted(pid, date);
                        break;
                    case "addDatastream":
                    case "modifyDatastreamByReference":
                    case "modifyDatastreamByValue":
                    case "purgeDatastream":
                    case "setDatastreamState":
                    case "setDatastreamVersionable":
                        updateTrackerPersistentStore.datastreamChanged(pid, date, param);
                        break;
                    case "addRelationship":
                    case "purgeRelationship":
                        updateTrackerPersistentStore.objectRelationsChanged(pid, date);
                        break;
                    case "getObjectXML":
                    case "export":
                    case "getDatastream":
                    case "getDatastreams":
                    case "getDatastreamHistory":
                    case "putTempStream":
                    case "getTempStream":
                    case "compareDatastreamChecksum":
                    case "getNextPID":
                    case "getRelationships":
                    case "validate":
                        // Nothing to do
                        break;
                    default:
                        // TODO log this
                        break;
                }
            }
        } catch (IOException | UpdateTrackerStorageException | FedoraFailedException e) {
            throw new RuntimeException(e);
        }
    }
}
