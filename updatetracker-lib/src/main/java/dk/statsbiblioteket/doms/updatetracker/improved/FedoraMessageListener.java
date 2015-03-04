package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedoraLog.DatabaseLogRetriever;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedoraLog.FedoraLogEvent;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

public class FedoraMessageListener extends TimerTask {

    private final DatabaseLogRetriever databaseLogRetriever;
    private final UpdateTrackerPersistentStore updateTrackerPersistentStore;
    private int limit;

    public FedoraMessageListener(DatabaseLogRetriever databaseLogRetriever, UpdateTrackerPersistentStore updateTrackerPersistentStore, int limit) {
        this.databaseLogRetriever = databaseLogRetriever;
        this.updateTrackerPersistentStore = updateTrackerPersistentStore;
        this.limit = limit;
    }

    @Override
    public void run() {
        try {
            Date lastRegisteredChange = updateTrackerPersistentStore.lastChanged();
            List<FedoraLogEvent> events = databaseLogRetriever.getFedoraEvents(lastRegisteredChange, limit);

            for (FedoraLogEvent event : events) {
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
