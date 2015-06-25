package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

/**
 * This timertasks regularly polls the fedora worklog and calls the update tracker with the unit of work
 */
public class WorkLogPollTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(WorkLogPollTask.class);

    private final WorkLogPollDAO workLogPollDAO;
    private final UpdateTrackerPersistentStore updateTrackerPersistentStore;
    private final int limit;
    private final int delay;

    /**
     * @param workLogPollDAO
     * @param updateTrackerPersistentStore
     * @param limit                        the amount of work units to retrieve in each invocation
     * @param delay                        the age (in seconds) of tasks before they are eligible for working on.
     */
    public WorkLogPollTask(WorkLogPollDAO workLogPollDAO, UpdateTrackerPersistentStore updateTrackerPersistentStore,
                           int limit, int delay) {
        this.workLogPollDAO = workLogPollDAO;
        this.updateTrackerPersistentStore = updateTrackerPersistentStore;
        this.limit = limit;
        this.delay = delay;
    }

    @Override
    public void run() {

        try {
            Long latestKey = updateTrackerPersistentStore.getLatestKey();

            List<WorkLogUnit> events = getEvents(latestKey);

            for (WorkLogUnit event : events) {
                try {
                    handleEvent(event);
                } catch(UpdateTrackerStorageException e){
                    log.error("Failed to store events in update tracker. Failed on '" + event + "'", e);
                    break; //If we fail, break the loop, as we DO NOT WANT to miss an event
                } catch(FedoraFailedException e){
                    log.error("Failed to communicate with fedora. Failed on '" + event + "'", e);
                    break; //If we fail, break the loop, as we DO NOT WANT to miss an event
                }
            }
            if (!events.isEmpty()) {
                log.info("Finished working on event list");
            }
        } catch (Exception e){
            //Fault barrier to avoid that this method bombs out
            //If this method bombs out, the timer is stopped, and will not start until the webservice is reloaded
            // So, we catch and log all exceptions, including runtime exceptions. Throwable and Errors will take the thing down hard, as they should
            log.error("Failed to poll for worklog tasks",e);
            //Log this and keep going. Only Errors get through now
        }
    }



    private List<WorkLogUnit> getEvents(Long lastRegisteredKey) {
        List<WorkLogUnit> events = new ArrayList<>();
        try {
            log.debug("Starting query for events since '{}'", lastRegisteredKey);
            events = workLogPollDAO.getFedoraEvents(lastRegisteredKey, limit, delay);
            log.info("Looking for events since '{}'. Found '{}", lastRegisteredKey, events.size());
        } catch (IOException e) {
            log.error("Failed to get Fedora events.", e);
        }
        return events;
    }

    private void handleEvent(WorkLogUnit event) throws UpdateTrackerStorageException, FedoraFailedException {
        long key = event.getKey();
        final String pid = event.getPid();
            final Date date = event.getDate();
            final String param = event.getParam();
            final String method = event.getMethod();
            log.debug("Registering the event '{}'", event);

        switch (method) {
            case "ingest":
                updateTrackerPersistentStore.objectCreated(pid, date, key);
                break;
            case "modifyObject":
                updateTrackerPersistentStore.objectStateChanged(pid, date, param, key);
                break;
            case "purgeObject":
                updateTrackerPersistentStore.objectDeleted(pid, date, key);
                break;
            case "addDatastream":
            case "modifyDatastreamByReference":
            case "modifyDatastreamByValue":
            case "purgeDatastream":
                updateTrackerPersistentStore.datastreamChanged(pid, date, param, key);
                break;
            case "setDatastreamState":
            case "setDatastreamVersionable":
                //Forget the datastream name, because VIEW and RELS-EXT should not trigger recalculation.
                updateTrackerPersistentStore.datastreamChanged(pid, date, null, key);
                break;
            case "addRelationship":
            case "purgeRelationship":
                updateTrackerPersistentStore.objectRelationsChanged(pid, date, key);
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
            case "validate": // Nothing to do
                log.debug("Got nonchanging event '{}' from worklog", event);
                break;
            default:
                log.warn("Got unknown event '{}' from worklog", event);
                break;
        }
    }
}
