package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * This class implements the lower level update tracker operations, detailed in
 * <a href="https://sbforge.org/display/DOMS/Update+Tracking#UpdateTracking-LowLevelChanges">https://sbforge
 * .org/display/DOMS/Update+Tracking#UpdateTracking-LowLevelChanges</a>
 * <br>
 * All methods in this class takes a session object as a parameter. They should have to care about transactions.
 *
 * @see dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl
 *
 */
public class UpdateTrackerBackend {
    private FedoraForUpdateTracker fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);


    public UpdateTrackerBackend(FedoraForUpdateTracker fedora) {


        this.fedora = fedora;
    }

    /**
     * Modify the persistent storage regarding a change.
     * @param pid     the pid of the object that was changed
     * @param timestamp    the timestamp of the change
     * @param collection
     * @param state   the state of the entries that should be updated
     * @param session
     */
    public void modifyState(String pid, Date timestamp, String collection, State state, Session session) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {
        log.debug("Starting modifyState({},{},{})", pid, timestamp, state);

        /*
        if new state is not Deleted
            If the object was not previously known in RECORDS
                if the object is an Entry object # Check content models vs. content model cache
                    Create row in RECORDS and OBJECTS denoting this Record, with the Inactive Timestamp set
                    if the new state is Active
                        also set the Active Timestamp
            else
                For each row in RECORDS with entryPid = this pid
                    set Inactive Timestamp
                    if new state is Active
                        set Active Timestamp
         */
        if ( state != State.DELETED) {
            List<Record> allRecordsWithThisEntryPid = UpdateTrackerDAO.getAllRecordsWithThisEntryPid(pid, session);
            if (allRecordsWithThisEntryPid.isEmpty()){
                List<String> entryAngles = fedora.getEntryAngles(pid, timestamp);
                for (String entryAngle : entryAngles) {
                    log.debug("Pid {} is an entry for viewangle {}", pid, entryAngle);
                    Record newRecord = new Record(pid, entryAngle, collection);
                    if (UpdateTrackerDAO.recordNotExists(session, newRecord)) {
                        log.debug("Pid {} is not marked as an entry for viewAngle {}. Fixing", pid, entryAngle);
                        final DomsObject object = UpdateTrackerDAO.loadOrCreate(session, pid);
                        newRecord.getObjects().add(object);
                        newRecord.setInactive(timestamp);
                        if (state == State.ACTIVE){
                            newRecord.setActive(timestamp);
                        }
                        session.saveOrUpdate(newRecord);
                    }
                }
            } else {
                for (Record recordWithThisEntryPid : allRecordsWithThisEntryPid) {
                    recordWithThisEntryPid.setInactive(timestamp);
                    if (state == State.ACTIVE) {
                        recordWithThisEntryPid.setActive(timestamp);
                    }
                    session.saveOrUpdate(recordWithThisEntryPid);
                }
            }
        }

        /*
        If the new state is Deleted
            Get all Records containing this pid from OBJECTS and not this pid as entryPid
            For each of these Records
                reconnectObjects(record.entryPid) # Recalculate the records
            Delete all rows with objectPid=this pid from OBJECTS # Remove reference to this object
            For each Record with entryPid = this pid # And mark is as deleted if it is an entry
                set Deleted Timestamp
                unset Active and Inactive Timestamp
         */

        else if (state == State.DELETED){
            log.debug("Switching on states for pid {}, got the Deleted branch", pid);

            //TODO This code duplicates code in recalc view
            DomsObject thisObject = UpdateTrackerDAO.loadOrCreate(session, pid);
            Set<Record> otherRecordsThanThisWhichThisObjectIsPart = removeThis(pid, thisObject);


            for (Record otherRecord : otherRecordsThanThisWhichThisObjectIsPart) {
                reconnectObjectsInRecord(timestamp, session, otherRecord);
            }
            session.delete(thisObject);

            Set<Record> recordsWhichThisObjectIsEntry = removeOthers(pid, thisObject);

            for (Record record : recordsWhichThisObjectIsEntry) {
                record.getObjects().clear();
                record.setDeleted(timestamp);
                record.setInactive(null);
                record.setActive(null);
                session.saveOrUpdate(record);
            }
        }

    }

    private Set<Record> removeOthers(final String pid, DomsObject thisObject) {
        final Set<Record> records = new HashSet<Record>(thisObject.getRecords());
        CollectionUtils.filter(records, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return object.getEntryPid().equals(pid); }
        });
        return records;
    }

    private Set<Record> removeThis(final String pid, DomsObject thisObject) {
        final Set<Record> records = new HashSet<Record>(thisObject.getRecords());
        CollectionUtils.filter(records, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return !object.getEntryPid().equals(pid); }
        });
        return records;
    }

    private void reconnectObjectsInRecord(Date timestamp, Session session, Record otherRecord) throws FedoraFailedException {
        ViewBundle bundle = fedora.calcViewBundle(otherRecord.getEntryPid(), otherRecord.getViewAngle(), timestamp);
        otherRecord.getObjects().clear();
        for (String viewObject : bundle.getContained()) {
            log.debug("Marking object {} as part of record {},{},{}", viewObject, otherRecord.getEntryPid(), otherRecord.getViewAngle(), otherRecord.getCollection());
            final DomsObject object = UpdateTrackerDAO.loadOrCreate(session, viewObject);
            otherRecord.getObjects().add(object);
        }

        if (otherRecord.getInactive().equals(otherRecord.getActive())){
            otherRecord.setActive(timestamp);
        }
        otherRecord.setInactive(timestamp);
        session.saveOrUpdate(otherRecord);
    }


    public void reconnectObjects(String pid, Date timestamp, Session session) throws
                                                                 FedoraFailedException,
                                                                 UpdateTrackerStorageException {
        /*
        Get the view Information about this object (Which viewAngles is this object entry for)
        get the Collection information about this object (which collections is it in)
        Create Records in RECORDS corresponding to all these view angles and collections (if they do not exist already) with the Inactive Timestamp set
        For each Record in RECORDS with this entry pid and not in this set of view angles or not in this set of collections
            Set Deleted Timestamp
            unset Inactive and Active Timestamp
            remove all objects from OBJECTS linked to this Record
        for each Record this object is part of (query OBJECTS with objectPid = this pull)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS
         */

        log.debug("starting reconnectObjects({},{})",pid,timestamp);

        //Create new Records
        final Collection<String> entryViewAngles = fedora.getEntryAngles(pid, timestamp);
        final Collection<String> collections = fedora.getCollections(pid, timestamp);
        for (String entryViewAngle : entryViewAngles) {
            for (String collection : collections) {
                Record record = new Record(pid, entryViewAngle, collection);
                if (UpdateTrackerDAO.recordNotExists(session, record)){
                    DomsObject object = UpdateTrackerDAO.loadOrCreate(session, pid);
                    record.getObjects().add(object);
                    record.setInactive(timestamp);
                    session.saveOrUpdate(record);
                }
            }
        }
        //Remove old records
        //"not (A and B)" is the same as "(not A) or (not B)"
        List<Record> previousRecords = UpdateTrackerDAO.getRecordsNotInTheseCollectionsAndViewAngles(pid,
                                                                                                     session,
                                                                                                     entryViewAngles,
                                                                                                     collections);

        for (Record previousRecord : previousRecords) {
            previousRecord.setDeleted(timestamp);
            previousRecord.setInactive(null);
            previousRecord.setActive(null);
            previousRecord.getObjects().clear();
            session.saveOrUpdate(previousRecord);
        }

        log.debug("Recalculating view for {}", pid);
        /*
        for each Record this object is part of (query OBJECTS with objectPid = this pid)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS

         */

        Set<Record> records = UpdateTrackerDAO.loadOrCreate(session, pid).getRecords();
        log.debug("Find all records {} containing {} ", records, pid);
        for (Record otherRecord : records) {
            reconnectObjectsInRecord(timestamp, session, otherRecord);
        }
    }

    public void updateDates(String pid, Date timestamp, Session session) {

            final Query query
                    = session.createQuery("update Record e " +
                                          "set" +
                                          " e.inactive=:timestamp," +
                                          " e.active=(case when e.active>=e.inactive then :timestamp else e.active end) " +
                                          "where :pid member of e.objects " +
                                          "and (e.deleted is null or e.inactive>=e.deleted)");
            query.setParameter("pid", pid);
            query.setParameter("timestamp", timestamp);
            query.executeUpdate();
    }

    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection,
                                StatelessSession session) {
        Query query;
        if (state == null) {
            query = session.getNamedQuery("All");
        } else {
            if (state.equals("A")) {
                query = session.getNamedQuery("ActiveAndDeleted");
            } else if (state.equals("I")) {
                query = session.getNamedQuery("InactiveOrDeleted");
            } else if (state.equals("D")) {
                query = session.getNamedQuery("Deleted");
            } else {
                query = session.getNamedQuery("All");
            }
        }


        query.setReadOnly(true);
        query.setFirstResult(offset)
             .setMaxResults(limit);
        query.setTimestamp("since", since)
             .setString("collection", collection)
             .setString("viewAngle", viewAngle);

        return UpdateTrackerDAO.listRecords(query);
    }

    public Date lastChanged(StatelessSession session) {

        final Query query = session.createQuery("select max(e.inactive) from Record e");
        query.setMaxResults(1);
        Object result = query.uniqueResult();
        if (result != null){
            if (result instanceof Date) {
                return (Date) result;
            }
        }
        return null;
    }
}
