package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private int limit;

    /**
     * @param workLogPollDAO
     * @param updateTrackerPersistentStore
     * @param limit                        the amount of work units to retrieve in each invocation
     */
    public WorkLogPollTask(WorkLogPollDAO workLogPollDAO, UpdateTrackerPersistentStore updateTrackerPersistentStore,
                           int limit) {
        this.workLogPollDAO = workLogPollDAO;
        this.updateTrackerPersistentStore = updateTrackerPersistentStore;
        this.limit = limit;
    }

    @Override
    public void run() {

        try {
            Long latestKey = workLogPollDAO.getLatestKey();

            List<WorkLogUnit> events = getEvents(latestKey);

            for (WorkLogUnit event : events) {
                try {
                    handleEvent(event);
                    workLogPollDAO.setLatestKey(event.getKey());
                } catch(IOException e){
                    log.error("Failed to update latestKey in database. Failed on '" + event + "'", e);
                    break; //If we fail, break the loop, as we DO NOT WANT to miss an event
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
        List<WorkLogUnit> events = new ArrayList<WorkLogUnit>();
        try {
            log.debug("Starting query for events since '{}'", lastRegisteredKey);
            events = workLogPollDAO.getFedoraEvents(lastRegisteredKey, limit);
            log.info("Looking for events since '{}'. Found '{}", lastRegisteredKey, events.size());
        } catch (IOException e) {
            log.error("Failed to get Fedora events.", e);
        }
        return events;
    }

    private void handleEvent(WorkLogUnit event) throws UpdateTrackerStorageException, FedoraFailedException {
            final String pid = event.getPid();
            final Date date = event.getDate();
            final String param = event.getParam();
            final String method = event.getMethod();
            log.debug("Registering the event '{}'", event);

            if (method.equals("ingest")) {
                updateTrackerPersistentStore.objectCreated(pid, date);
            } else if (method.equals("modifyObject")) {
                updateTrackerPersistentStore.objectStateChanged(pid, date, param);
            } else if (method.equals("purgeObject")) {
                updateTrackerPersistentStore.objectDeleted(pid, date);
            } else if (method.equals("addDatastream") ||
                       method.equals("modifyDatastreamByReference") ||
                       method.equals("modifyDatastreamByValue") ||
                       method.equals("purgeDatastream") ||
                       method.equals("setDatastreamState") ||
                       method.equals("setDatastreamVersionable")) {
                updateTrackerPersistentStore.datastreamChanged(pid, date, param);
            } else if (method.equals("addRelationship") || method.equals("purgeRelationship")) {
                updateTrackerPersistentStore.objectRelationsChanged(pid, date);
            } else if (method.equals("getObjectXML") || method.equals("export") ||
                       method.equals("getDatastream") ||
                       method.equals("getDatastreams") ||
                       method.equals("getDatastreamHistory") ||
                       method.equals("putTempStream") ||
                       method.equals("getTempStream") ||
                       method.equals("compareDatastreamChecksum") ||
                       method.equals("getNextPID") || method.equals("getRelationships") ||
                       method.equals("validate")) {// Nothing to do
                log.debug("Got nonchanging event '{}' from worklog", event);
            } else {
                log.warn("Got unknown event '{}' from worklog", event);
            }
    }
}
