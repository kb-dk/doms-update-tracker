package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

/**
 * This timertasks regularly polls the fedora worklog and calls the update tracker with the unit of work
 */
public class WorkLogPollTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(WorkLogPollTask.class);

    private final WorkLogPoller workLogPoller;
    private final UpdateTrackerPersistentStore updateTrackerPersistentStore;
    private int limit;

    /**
     * @param workLogPoller
     * @param updateTrackerPersistentStore
     * @param limit                        the amount of work units to retrieve in each invocation
     */
    public WorkLogPollTask(WorkLogPoller workLogPoller, UpdateTrackerPersistentStore updateTrackerPersistentStore,
                           int limit) {
        this.workLogPoller = workLogPoller;
        this.updateTrackerPersistentStore = updateTrackerPersistentStore;
        this.limit = limit;
    }

    @Override
    public void run() {
        try {
            //TODO DO NOT USE LASTMODIFIED, USE THE INCREMENTING KEY
            Date lastRegisteredChange = updateTrackerPersistentStore.lastChanged();
            if (lastRegisteredChange == null){
                lastRegisteredChange = new Date(0);
            }
            log.info("Looking for events since '{}'",lastRegisteredChange);
            List<WorkLogUnit> events = workLogPoller.getFedoraEvents(lastRegisteredChange, limit);
            log.info("Found  '{}' events ", events.size());

            for (WorkLogUnit event : events) {
                String pid = event.getPid();
                Date date = event.getDate();
                String param = event.getParam();
                log.info("Registering the event '{}'",event);

                if (event.getMethod().equals("ingest")) {
                    updateTrackerPersistentStore.objectCreated(pid, date);
                } else if (event.getMethod().equals("modifyObject")) {
                    updateTrackerPersistentStore.objectStateChanged(pid, date, param);
                } else if (event.getMethod().equals("purgeObject")) {
                    updateTrackerPersistentStore.objectDeleted(pid, date);
                } else if (event.getMethod().equals("addDatastream") ||
                           event.getMethod().equals("modifyDatastreamByReference") ||
                           event.getMethod().equals("modifyDatastreamByValue") ||
                           event.getMethod().equals("purgeDatastream") ||
                           event.getMethod().equals("setDatastreamState") ||
                           event.getMethod().equals("setDatastreamVersionable")) {
                    updateTrackerPersistentStore.datastreamChanged(pid, date, param);
                } else if (event.getMethod().equals("addRelationship") ||
                           event.getMethod().equals("purgeRelationship")) {
                    updateTrackerPersistentStore.objectRelationsChanged(pid, date);
                } else if (event.getMethod().equals("getObjectXML") || event.getMethod().equals("export") ||
                           event.getMethod().equals("getDatastream") ||
                           event.getMethod().equals("getDatastreams") ||
                           event.getMethod().equals("getDatastreamHistory") ||
                           event.getMethod().equals("putTempStream") ||
                           event.getMethod().equals("getTempStream") ||
                           event.getMethod().equals("compareDatastreamChecksum") ||
                           event.getMethod().equals("getNextPID") || event.getMethod().equals("getRelationships") ||
                           event.getMethod().equals("validate")) {// Nothing to do

                } else {// TODO log this

                }
            }
            log.info("No further events to work on");
        } catch (IOException e) {
            log.error("Failed to get Fedora events",e);
        } catch (UpdateTrackerStorageException e) {
            log.error("Failed to store events in update tracker", e);
        } catch (FedoraFailedException e) {
            log.error("Failed to communicate with fedora", e);
        }
    }
}
