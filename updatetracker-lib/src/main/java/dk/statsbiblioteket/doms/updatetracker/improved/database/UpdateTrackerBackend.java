package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.util.caching.TimeSensitiveCache;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Id;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


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
    private final ExecutorService viewBundleThreadPool;
    private FedoraForUpdateTracker fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);

    private final Map<String,ViewBundle> viewBundleCache;

    public UpdateTrackerBackend(FedoraForUpdateTracker fedora, Long viewBundleCacheTime, ExecutorService viewBundleThreadPool) {
        this.viewBundleThreadPool = viewBundleThreadPool;
        viewBundleCache = new TimeSensitiveCache<>(viewBundleCacheTime, true);
        this.fedora = fedora;
    }

    /**
     * Modify the persistent storage regarding a change.
     * @param pid     the pid of the object that was changed
     * @param timestamp    the timestamp of the change
     * @param collection the collection of the object
     * @param state   the state of the entries that should be updated
     * @param db the database access object
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
            Collection<Record> allRecordsWithThisEntryPid = db.getAllRecordsWithThisEntryPid(pid);
            if (allRecordsWithThisEntryPid.isEmpty()){
                Collection<String> entryAngles = fedora.getEntryAngles(pid, timestamp);
                for (String entryAngle : entryAngles) {
                    log.debug("Pid {} is an entry for viewangle {}", pid, entryAngle);

                    Record newRecord = db.getPersistentRecord(new Record(pid, entryAngle, collection));
                    if (newRecord == null) {
                        newRecord = new Record(pid, entryAngle, collection);
                        log.debug("Pid {} is not marked as an entry for viewAngle {}. Fixing", pid, entryAngle);
                    }
                    newRecord.getObjects().add(pid);
                    newRecord.setInactive(timestamp);
                    if (state == State.ACTIVE){
                        newRecord.setActive(timestamp);
                    }
                    db.saveRecord(newRecord);
                }
            } else {
                for (Record recordWithThisEntryPid : allRecordsWithThisEntryPid) {
                    recordWithThisEntryPid.setInactive(timestamp);
                    if (state == State.ACTIVE) {
                        recordWithThisEntryPid.setActive(timestamp);
                    }
                    db.saveRecord(recordWithThisEntryPid);
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

            final Collection<Record> records = db.getRecordsContainingThisPid(pid);

            Set<Record> otherRecordsThanThisWhichThisObjectIsPart = getRecordsWithoutThisPidAsEntry(pid, records);
            Set<Record> changes = recalculateRecords(timestamp, otherRecordsThanThisWhichThisObjectIsPart);
            for (Record change : changes) {
                db.saveRecord(change);
            }
            Set<Record> recordsWhichThisObjectIsEntry = getRecordWithThisPidAsEntry(pid, records);

            for (Record record : recordsWhichThisObjectIsEntry) {
                record.getObjects().clear();
                record.setDeleted(timestamp);
                record.setInactive(null);
                record.setActive(null);
                db.saveRecord(record);
            }
        }

    }

    private Set<Record> getRecordWithThisPidAsEntry(final String pid, Collection<Record> records) {
        final Set<Record> coll = new HashSet<>(records);
        CollectionUtils.filter(coll, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return object.getEntryPid().equals(pid); }
        });
        return coll;
    }

    private Set<Record> getRecordsWithoutThisPidAsEntry(final String pid, Collection<Record> records) {
        final Set<Record> coll = new HashSet<>(records);
        CollectionUtils.filter(coll, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return !object.getEntryPid().equals(pid); }
        });
        return coll;
    }

    private Set<Record> recalculateRecords(final Date timestamp,
                                           Set<Record> records) throws FedoraFailedException {
        Set<Future<Record>> recordsToSave = new HashSet<>();
        for (final Record record : records) {
            Callable<Record> reconnector = new RecordReconnector(record, timestamp);
            recordsToSave.add(viewBundleThreadPool.submit(reconnector));
        }
        Set<Record> result = new HashSet<>();
        for (Future<Record> recordFuture : recordsToSave) {
            Record record;
            try {
                record = recordFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FedoraFailedException("Failed while getting view bundles", e);
            }
            if (record != null) {
                result.add(record);
            }
        }
        return result;
    }

    protected ViewBundle getViewBundle(Date timestamp, Record record) throws FedoraFailedException {
        final String key = toKey(record, timestamp);
        ViewBundle bundle = viewBundleCache.get(key);
        if (bundle == null){
            bundle = fedora.calcViewBundle(record.getEntryPid(), record.getViewAngle(), timestamp);
            viewBundleCache.put(key,bundle);
        }
        return bundle;
    }

    protected static String toKey(Record record, Date timestamp) {
        StringBuilder key = new StringBuilder();
        for (Field field : Record.class.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null) {
                try {
                    String methodName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
                    Method getterMethod = Record.class.getDeclaredMethod(methodName);
                    Object value = getterMethod.invoke(record);
                    key.append(value).append(",");
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Failed to resolve key of record "+record,e);
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    try {
                        key.append(field.get(record)).append(",");
                    } catch (IllegalAccessException e1) {
                        //No getter and not public, ignore
                    }
                }
            }
        }
        for (Method method : Record.class.getMethods()){
            if (method.getParameterTypes().length == 0){
                if (method.getAnnotation(Id.class) != null){
                    try {
                        key.append(method.invoke(record)).append(",");
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException("Failed to resolve key of record "+record,e);
                    }
                }
            }
        }
        key.append(timestamp.getTime());
        return key.toString();
    }


    /**
     * This methods returns a set of record objects that should be changed when this pid have changed
     * @param pid the pid that changed
     * @param timestamp the timestamp
     * @param db the database db
     * @param collections collections which this pid belongs to
     * @return a set of records to save
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     */
    public Set<Record> recalculateRecordsBasedOnThisPid(String pid, Date timestamp, DB db,
                                                        Collection<String> collections, State state) throws
                                                                 FedoraFailedException,
                                                                 UpdateTrackerStorageException {
        Set<Record> result = new HashSet<>();
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
        Set<Record> newRecords = new HashSet<>();
        //Create new Records
        final Collection<String> entryViewAngles = fedora.getEntryAngles(pid, timestamp);
        for (String entryViewAngle : entryViewAngles) {
            for (String collection : collections) {
                Record record = new Record(pid, entryViewAngle, collection);
                if ((record = db.getPersistentRecord(record)) == null){
                    record = new Record(pid, entryViewAngle, collection);
                    record.getObjects().add(pid);
                    record.setInactive(timestamp);
                    result.add(record);
                    if (state == State.ACTIVE) {
                        record.setActive(timestamp);
                    }
                }
                newRecords.add(record);
            }
        }
        //Remove old records
        //"not (A and B)" is the same as "(not A) or (not B)"
        Collection<Record> previousRecords = db.getRecordsNotInTheseCollectionsAndViewAngles(pid, entryViewAngles, collections);

        for (Record previousRecord : previousRecords) {
            previousRecord.setDeleted(timestamp);
            previousRecord.setInactive(null);
            previousRecord.setActive(null);
            previousRecord.getObjects().clear();
            result.add(previousRecord);
        }

        log.debug("Recalculating view for {}", pid);
        /*
        for each Record this object is part of (query OBJECTS with objectPid = this pid)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS

         */

        Set<Record> records = new HashSet<>(db.getRecordsContainingThisPid(pid));
        //Since the database connection have not been flushed, the newly created records will not be found, so add them
        records.addAll(newRecords);
        log.debug("Find all records {} containing {} ", records, pid);
        result.addAll(recalculateRecords(timestamp, records));
        return result;
    }

    public void updateDates(String pid, Date timestamp, DB db) {
        db.updateDates(pid, timestamp);
    }

    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection,
                               DB db) {
        return db.lookup(since, viewAngle, offset, limit, state, collection);
    }

    public Date lastChanged(DB db, String viewangle, String collection) {
        return db.getLastChangedTimestamp(viewangle, collection);
    }

    private class RecordReconnector implements Callable<Record> {
        private final Record record;
        private final Date timestamp;

        public RecordReconnector(Record record, Date timestamp) {
            this.record = record;
            this.timestamp = timestamp;
        }

        @Override
        public Record call() throws Exception {
            if (record.getState() != State.DELETED) {
                Set<String> before = new HashSet<>(record.getObjects());
                Set<String> after = new HashSet<>();
                ViewBundle bundle = getViewBundle(timestamp, record);
                for (String viewObject : bundle.getContained()) {
                    log.debug("Marking object {} as part of record {},{},{}",
                              viewObject,
                              record.getEntryPid(),
                              record.getViewAngle(),
                              record.getCollection());
                    after.add(viewObject);
                }

                if (!before.equals(after)) {
                    record.getObjects().clear();
                    record.getObjects().addAll(after);
                    if (record.getInactive() != null &&
                        record.getActive() != null &&
                        record.getInactive().equals(record.getActive())) {
                        record.setActive(timestamp);
                    }
                    record.setInactive(timestamp);
                    return record;
                }
            }
            return null;//This marks that no update should be done
        }
    }
}
