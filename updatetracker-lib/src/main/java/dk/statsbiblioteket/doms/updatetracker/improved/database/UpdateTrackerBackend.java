package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.StatelessDB;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.util.Pair;
import dk.statsbiblioteket.util.caching.TimeSensitiveCache;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.hibernate.Query;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
//TODO test if these operations are idempotent. Can we replay the same set of operations for the same result?
public class UpdateTrackerBackend {
    private FedoraForUpdateTracker fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);

    private final Map<Pair<Record,Date>,ViewBundle> viewBundleCache;

    public UpdateTrackerBackend(FedoraForUpdateTracker fedora, Long viewBundleCacheTime) {
        viewBundleCache = new TimeSensitiveCache<Pair<Record,Date>, ViewBundle>(viewBundleCacheTime, true);
        this.fedora = fedora;
    }

    /**
     * Modify the persistent storage regarding a change.
     * @param pid     the pid of the object that was changed
     * @param timestamp    the timestamp of the change
     * @param collection
     * @param state   the state of the entries that should be updated
     * @param db
     */
    public void modifyState(String pid, Date timestamp, String collection, State state, DB db) throws
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
            List<Record> allRecordsWithThisEntryPid = db.getAllRecordsWithThisEntryPid(pid);
            if (allRecordsWithThisEntryPid.isEmpty()){
                List<String> entryAngles = fedora.getEntryAngles(pid, timestamp);
                for (String entryAngle : entryAngles) {
                    log.debug("Pid {} is an entry for viewangle {}", pid, entryAngle);
                    Record newRecord = new Record(pid, entryAngle, collection);
                    if (db.recordNotExists(newRecord)) {
                        log.debug("Pid {} is not marked as an entry for viewAngle {}. Fixing", pid, entryAngle);
                        newRecord.getObjects().add(pid);
                        newRecord.setInactive(timestamp);
                        if (state == State.ACTIVE){
                            newRecord.setActive(timestamp);
                        }
                        db.saveOrUpdate(newRecord);
                    }
                }
            } else {
                for (Record recordWithThisEntryPid : allRecordsWithThisEntryPid) {
                    recordWithThisEntryPid.setInactive(timestamp);
                    if (state == State.ACTIVE) {
                        recordWithThisEntryPid.setActive(timestamp);
                    }
                    db.saveOrUpdate(recordWithThisEntryPid);
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

            final Collection<Record> records = db.getRecordsForPid(pid);

            Set<Record> otherRecordsThanThisWhichThisObjectIsPart = recordsWithoutThisPidAsEntry(pid, records);


            for (Record otherRecord : otherRecordsThanThisWhichThisObjectIsPart) {
                if (otherRecord.getState() != State.DELETED) {
                    reconnectObjectsInRecord(timestamp, db, otherRecord);
                }
            }

            Set<Record> recordsWhichThisObjectIsEntry = recordWithThisPidAsEntry(pid, records);

            for (Record record : recordsWhichThisObjectIsEntry) {
                record.getObjects().clear();
                record.setDeleted(timestamp);
                record.setInactive(null);
                record.setActive(null);
                db.saveOrUpdate(record);
            }
        }

    }

    private Set<Record> recordWithThisPidAsEntry(final String pid, Collection<Record> records) {
        final Set<Record> coll = new HashSet<Record>(records);
        CollectionUtils.filter(coll, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return object.getEntryPid().equals(pid); }
        });
        return coll;
    }

    private Set<Record> recordsWithoutThisPidAsEntry(final String pid, Collection<Record> records) {
        final Set<Record> coll = new HashSet<Record>(records);
        CollectionUtils.filter(coll, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return !object.getEntryPid().equals(pid); }
        });
        return coll;
    }

    private void reconnectObjectsInRecord(Date timestamp, DB db, Record otherRecord) throws FedoraFailedException {
        Set<String> before = new HashSet<String>(otherRecord.getObjects());
        Set<String> after = new HashSet<String>();
        ViewBundle bundle = getViewBundle(timestamp, otherRecord);
        for (String viewObject : bundle.getContained()) {
            log.debug("Marking object {} as part of record {},{},{}", viewObject, otherRecord.getEntryPid(), otherRecord.getViewAngle(), otherRecord.getCollection());
            after.add(viewObject);
        }

        if (!before.equals(after)) {
            otherRecord.getObjects().clear();
            otherRecord.getObjects().addAll(after);
            if (otherRecord.getInactive() != null &&
                otherRecord.getActive() != null &&
                otherRecord.getInactive().equals(otherRecord.getActive())) {
                otherRecord.setActive(timestamp);
            }
            otherRecord.setInactive(timestamp);
            db.saveOrUpdate(otherRecord);
        }
    }

    private ViewBundle getViewBundle(Date timestamp, Record otherRecord) throws FedoraFailedException {
        final Pair<Record, Date> key = new Pair<Record, Date>(otherRecord, timestamp);
        ViewBundle bundle = viewBundleCache.get(key);
        if (bundle == null){
            bundle = fedora.calcViewBundle(otherRecord.getEntryPid(), otherRecord.getViewAngle(), timestamp);
            viewBundleCache.put(key,bundle);
        }
        return bundle;
    }


    public void reconnectObjects(String pid, Date timestamp, DB db, Collection<String> collections) throws
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

        //Store the new records so that we do not need to flush the database to find them again
        Set<Record> newRecords = new HashSet<Record>();
        //Create new Records
        final Collection<String> entryViewAngles = fedora.getEntryAngles(pid, timestamp);
        for (String entryViewAngle : entryViewAngles) {
            for (String collection : collections) {
                Record record = new Record(pid, entryViewAngle, collection);
                if (db.recordNotExists(record)){
                    record.getObjects().add(pid);
                    record.setInactive(timestamp);
                    db.saveOrUpdate(record);
                    newRecords.add(record);
                }
            }
        }
        //Remove old records
        //"not (A and B)" is the same as "(not A) or (not B)"
        List<Record> previousRecords = db.getRecordsNotInTheseCollectionsAndViewAngles(pid, entryViewAngles, collections);

        for (Record previousRecord : previousRecords) {
            previousRecord.setDeleted(timestamp);
            previousRecord.setInactive(null);
            previousRecord.setActive(null);
            previousRecord.getObjects().clear();
            db.saveOrUpdate(previousRecord);
        }

        log.debug("Recalculating view for {}", pid);
        /*
        for each Record this object is part of (query OBJECTS with objectPid = this pid)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS

         */

        Set<Record> records = new HashSet<Record>(db.getRecordsForPid(pid));
        //Since the database connection have not been flushed, the newly created records will not be found, so add them
        records.addAll(newRecords);
        log.debug("Find all records {} containing {} ", records, pid);
        for (Record otherRecord : records) {
            if (otherRecord.getState() != State.DELETED) {
                reconnectObjectsInRecord(timestamp, db, otherRecord);
            }
        }
    }

    public void updateDates(String pid, Date timestamp, DB db) {
        db.updateDates(pid, timestamp);
    }

    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection,
                               StatelessDB session) {
        return session.lookup(since, viewAngle, offset, limit, state, collection);
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
