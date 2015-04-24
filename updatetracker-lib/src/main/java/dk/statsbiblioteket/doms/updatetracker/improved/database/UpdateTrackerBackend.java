package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.util.Pair;
import dk.statsbiblioteket.util.caching.TimeSensitiveCache;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;


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
public class UpdateTrackerBackend implements Closeable{
    private final ExecutorService viewBundleThreadPool;
    private FedoraForUpdateTracker fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);

    private final Map<Pair<Record,Date>,ViewBundle> viewBundleCache;

    public UpdateTrackerBackend(FedoraForUpdateTracker fedora, Long viewBundleCacheTime) {
        viewBundleCache = new TimeSensitiveCache<>(viewBundleCacheTime, true);
        this.fedora = fedora;
        //This creates the thread pool for view reconnection.
        //The issue here is that the database session is not thread safe, but the fedora service is. Therefore, we must
        //recalculate the view multithreaded, but we must save the results in the main thread
        viewBundleThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override //Hack to make the threads daemon threads so they do not block shutdown
            public Thread newThread(Runnable r) {
                ThreadFactory fac = Executors.defaultThreadFactory();
                Thread thread = fac.newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
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
                        newRecord.getObjects().add(pid);
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

            final Collection<Record> records = UpdateTrackerDAO.getRecordsForPid(session, pid);

            Set<Record> otherRecordsThanThisWhichThisObjectIsPart = recordsWithoutThisPidAsEntry(pid, records);
            reconnectTheseRecords(timestamp, session, otherRecordsThanThisWhichThisObjectIsPart);
            Set<Record> recordsWhichThisObjectIsEntry = recordWithThisPidAsEntry(pid, records);

            for (Record record : recordsWhichThisObjectIsEntry) {
                record.getObjects().clear();
                record.setDeleted(timestamp);
                record.setInactive(null);
                record.setActive(null);
                session.saveOrUpdate(record);
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
        final Set<Record> coll = new HashSet<>(records);
        CollectionUtils.filter(coll, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record object) { return !object.getEntryPid().equals(pid); }
        });
        return coll;
    }

    private void reconnectTheseRecords(final Date timestamp, Session session,
                                       Set<Record> records) throws FedoraFailedException {
        Set<Future<Record>> recordsToSave = new HashSet<>();
        for (final Record record : records) {
            Callable<Record> reconnector = new Callable<Record>() {
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
            };
            recordsToSave.add(viewBundleThreadPool.submit(reconnector));
        }
        for (Future<Record> recordFuture : recordsToSave) {
            Record record;
            try {
                record = recordFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FedoraFailedException("Failed while getting view bundles", e);
            }
            if (record != null) {
                session.saveOrUpdate(record);
            }
        }
    }

    private ViewBundle getViewBundle(Date timestamp, Record otherRecord) throws FedoraFailedException {
        final Pair<Record, Date> key = new Pair<>(otherRecord, timestamp);
        ViewBundle bundle = viewBundleCache.get(key);
        if (bundle == null){
            bundle = fedora.calcViewBundle(otherRecord.getEntryPid(), otherRecord.getViewAngle(), timestamp);
            viewBundleCache.put(key,bundle);
        }
        return bundle;
    }


    public void reconnectObjects(String pid, Date timestamp, Session session, Collection<String> collections) throws
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
        Set<Record> newRecords = new HashSet<>();
        //Create new Records
        final Collection<String> entryViewAngles = fedora.getEntryAngles(pid, timestamp);
        for (String entryViewAngle : entryViewAngles) {
            for (String collection : collections) {
                Record record = new Record(pid, entryViewAngle, collection);
                if (UpdateTrackerDAO.recordNotExists(session, record)){
                    record.getObjects().add(pid);
                    record.setInactive(timestamp);
                    session.saveOrUpdate(record);
                    newRecords.add(record);
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

        Set<Record> records = new HashSet<>(UpdateTrackerDAO.getRecordsForPid(session,pid));
        //Since the database connection have not been flushed, the newly created records will not be found, so add them
        records.addAll(newRecords);
        log.debug("Find all records {} containing {} ", records, pid);
        reconnectTheseRecords(timestamp, session, records);
    }

    public void updateDates(String pid, Date timestamp, Session session) {
        final Query query = session.getNamedQuery("UpdateDates");
        query.setParameter("pid", pid);
        query.setParameter("timestamp", timestamp);
        query.setParameter("now",new Date());
        query.executeUpdate();
    }

    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection,
                               StatelessSession session) {
        Query query;
        if (state == null) {
            query = session.getNamedQuery("All");
        } else {
            final State fromName = State.fromName(state);
            if (fromName == null){
                query = session.getNamedQuery("All");
            } else {
                switch (fromName) {
                    case ACTIVE:
                        query = session.getNamedQuery("ActiveAndDeleted");
                        break;
                    case INACTIVE:
                        query = session.getNamedQuery("InactiveOrDeleted");
                        break;
                    case DELETED:
                        query = session.getNamedQuery("Deleted");
                        break;
                    default:
                        query = session.getNamedQuery("All");
                        break;
                }
            }
        }


        query.setReadOnly(true);
        query.setFirstResult(offset);
        query.setTimestamp("since", since)
             .setString("collection", collection)
             .setString("viewAngle", viewAngle)
             .setLong("limit", limit);

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

    @Override
    public void close() throws IOException {
        viewBundleThreadPool.shutdown();
    }
}
