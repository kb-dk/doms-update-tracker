package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.hibernate.HibernateException;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State.DELETED;

/**
 * This class correspond the the operations detailed in <a href="https://sbforge.org/display/DOMS/Update+Tracking#UpdateTracking-HighLevelChanges">https://sbforge.org/display/DOMS/Update+Tracking#UpdateTracking-HighLevelChanges</a>
 * <br>
 *
 * The lower level operations are detailed in UpdateTrackerBackend
 *
 * This class handles the database sessions, and commits or rolls back the started transactions.
 *
 * TODO the javadoc in this class corresponds to the linked wiki page. Make sure the code correspond to the javadoc
 *
 * @see dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerBackend
 */
public class UpdateTrackerPersistentStoreImpl implements UpdateTrackerPersistentStore, Closeable {

    private static Logger log = LoggerFactory.getLogger(UpdateTrackerPersistentStoreImpl.class);
    private final DBFactory dbfac;


    private FedoraForUpdateTracker fedora;
    private UpdateTrackerBackend backend;
    private final ExecutorService threadPool;


    public UpdateTrackerPersistentStoreImpl(FedoraForUpdateTracker fedora,
                                            UpdateTrackerBackend backend,
                                            DBFactory dbfac,
                                            ExecutorService threadPool) {
        this.fedora = fedora;
        this.backend = backend;
        this.threadPool = threadPool;
        this.dbfac = dbfac;

    }

    /**
     * The object  was created.
     *
     * Object Created: The Object was created in DOMS
     *   Fedora operations:
     *       - ingest
     *   Action:
     *       modifyState(Inactive)
     *       reconnectObjects()
     *       updateTimestamps()
     *
     *  @param pid  the pid of the new object
     * @param timestamp the date of the object creation
     * @param key the key from the work log table, that defined this operation.
     */
    @Override
    public void objectCreated(String pid, Date timestamp, long key) throws UpdateTrackerStorageException, FedoraFailedException {
        DB db = dbfac.createDBConnection();
        Transaction transaction = db.beginTransaction();
        log.debug("ObjectCreated({},{}) Starting",pid,timestamp);
        try {
            Set<String> collections = fedora.getCollections(pid, timestamp);
            State ingestState = fedora.getState(pid, timestamp);
            for (String collection : collections) {
                backend.modifyState(pid, timestamp, collection, ingestState, db);
            }
            Set<Record> changedRecords = backend.recalculateRecordsBasedOnThisPid(pid,
                                                                                  timestamp,
                                                                                  db,
                                                                                  collections,
                                                                                  ingestState);
            for (Record changedRecord : changedRecords) {
                db.saveRecord(changedRecord);
            }
            backend.updateDates(pid, timestamp, db);

            db.setLatestKey(key);
            transaction.commit();
            log.info("ObjectCreated({},{}) Completed", pid, timestamp);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he){
                log.error("Failed to rollback transaction",he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object created for pid='" + pid +
                                                    "' at date='" + timestamp.getTime() + "'", e);
        }

    }

    /**
     * The object was deleted
     *
     * Object Deleted: The Object was purged from DOMS
     *      Fedora operations:
     *           - purgeObject
     *      Action:
     *           modifyState(Deleted)
     *           updateDates()
     *           if content model
     *              for all objects of this class
     *                  reconnectObjects()
     *                  updateDate()
     *
     *
     *  If you purge a content model, that people still link to, you are breaking the link structure, and I feel the
     *  update tracker is justified in ignoring it. If nobody links to the content model, it does not matter that you
     *  purged it.
     If you just changed the state to Deleted, it does, per definition, not matter
     TODO update the wiki page
     * @param pid  the pid of the object
     * @param timestamp the date of the change
     * @param key the key from the work log table, that defined this operation.
     */
    @Override
    public void objectDeleted(String pid, Date timestamp, long key) throws UpdateTrackerStorageException, FedoraFailedException {
        DB db = dbfac.createDBConnection();
        Transaction transaction = db.beginTransaction();
        log.debug("ObjectDeleted({},{}) Starting", pid, timestamp);
        try {
            backend.modifyState(pid, timestamp, null, DELETED, db);
            backend.updateDates(pid, timestamp, db);
            db.setLatestKey(key);
            transaction.commit();
            log.info("ObjectDeleted({},{}) Completed", pid, timestamp);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object deleted for pid='" + pid +
                                                    "' at date='" + timestamp.getTime() + "'", e);
        }
    }

    /**
     * Datastream Changed: The Object datastreams changed. Handled differently depending on whether this is the relations datastream
     *      Fedora operations:
     *           - addDatastream
     *           - modifyDatastreamByReference
     *           - modifyDatastreamByValue
     *           - purgeDatastream
     *           - setDatastreamState
     *           - setDatastreamVersionable
     *           - updateDate
     *      Action:
     *           if RELS-EXT
     *                reconnectObjects(this)
     *           fi
     *           updateDate(this)
     *           if VIEW and Content Model
     *                for all objects of this class
     *                     reconnectObjects(object)
     *                     updateDate(object)
     fi
     * @param pid the pid of the object
     * @param timestamp the timestamp of the event
     * @param dsid the id of the datastream that changed
     * @param key the key from the work log table, that defined this operation.
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void datastreamChanged(String pid, Date timestamp, String dsid, long key) throws
                                                                      UpdateTrackerStorageException,
                                                                      FedoraFailedException {
        DB db = dbfac.createDBConnection();
        Transaction transaction = db.beginTransaction();
        log.debug("DatastreamChanged({},{},{}) Starting", pid, timestamp,dsid);
        try {
            if (dsid != null) {
                if ((dsid.equals("VIEW") || dsid.equals("RELS-EXT"))) {
                    if (dsid.equals("RELS-EXT")) {
                        Set<String> collections = fedora.getCollections(pid, timestamp);
                        State state = fedora.getState(pid, timestamp);
                        Set<Record> changedRecords = backend.recalculateRecordsBasedOnThisPid(pid,
                                                                                              timestamp,
                                                                                              db,
                                                                                              collections,
                                                                                              state);
                        for (Record changedRecord : changedRecords) {
                            db.saveRecord(changedRecord);
                        }
                    }
                }
            }
            backend.updateDates(pid, timestamp, db);
            db.setLatestKey(key);
            transaction.commit();
            log.info("DatastreamChanged({},{},{}) Completed", pid, timestamp, dsid);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in datastream changed for pid='" + pid +
                                                    "' at date='" + timestamp.getTime() + "' and dsid='"+dsid+"'", e);
        }
    }

    private void contentModelChanged(String contentModel, final Date timestamp, DB db) throws
                                                                                                 FedoraFailedException
    {
        fedora.invalidateContentModel(contentModel);

        ExecutorCompletionService<Set<Record>> progressTracker = new ExecutorCompletionService<>(threadPool);

        final Set<String> objectsOfThisContentModel = fedora.getObjectsOfThisContentModel(contentModel);//TODO this set can be HUGE
        Set<Future<Set<Record>>> results = new HashSet<>(objectsOfThisContentModel.size());

        for (final String object : objectsOfThisContentModel) {
            Callable<Set<Record>> reconnector = new ObjectOfContentModelReconnector(object, timestamp, dbfac);
            results.add(progressTracker.submit(reconnector));
        }
        int records = 0;
        for (int futureCount = 0; futureCount < objectsOfThisContentModel.size(); futureCount++) {
            Set<Record> changedRecords;
            changedRecords = getCompleted(contentModel, results, progressTracker);

            for (Record changedRecord : changedRecords) { //We log progress on merge back, not on calc of view
                log.debug("Working on object nr {} out of {} of content model {}",
                          records++,
                          objectsOfThisContentModel.size(),
                          contentModel);
                db.saveRecord(changedRecord);//Merge them to this session, and since they are changed, they will be changed in the commit
                backend.updateDates(changedRecord.getEntryPid(), timestamp, db);
            }

        }
    }



    /**
     * The object's relations changed. This can cause a recalculation of the viewStructure.
     *
     * Object Relations Changed: The Object changed in a fashion that DOES require the view to be recomputed.
     *      Fedora operations:
     *           - addRelationship
     *           - purgeRelationship
     *      Action:
     *           reconnectObjects(this)
     *           updateDate(this)
     *           if this is a content model
     *                for all objects of this class
     *                     reconnectObjects(object of this class)
     *                     updateDate(object of this class)
     *  @param pid  the pid of the object that changed
     * @param timestamp the date of the change
     * @param key the key from the work log table, that defined this operation.
     */
    public void objectRelationsChanged(String pid, Date timestamp, long key) throws
                                                              UpdateTrackerStorageException,
                                                              FedoraFailedException {
        if (pid.contains("/")) {
            //This means that the pid is really a datastream ID, i.e. that the relationsship is in RELS-INT
            final String[] split = pid.split("/");
            final String realPid = split[0];
            final String datastream = split[1];
            datastreamChanged(realPid, timestamp, "RELS-INT", key);
        } else {
            datastreamChanged(pid, timestamp, "RELS-EXT", key);
        }
    }


    /**
     * Object State Changed: The Object changed state in DOMS
     *      Fedora operations:
     *           - modifyObject
     *       Action:
     *            modifyState(state)
     *            updateDate()
     *
     * @param pid the pid of the object
     * @param timestamp the timestamp of the event
     * @param newstate the new state of the object
     * @param key the key from the work log table, that defined this operation.
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void objectStateChanged(String pid, Date timestamp, String newstate, long key) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {

        DB db = dbfac.createDBConnection();
        Transaction transaction = db.beginTransaction();
        log.debug("objectStateChanged({},{},{}) Starting", pid, timestamp, newstate);
        try {
            Set<String> collections = fedora.getCollections(pid, timestamp);
            for (String collection : collections) {
                backend.modifyState(pid, timestamp, collection, State.fromName(newstate), db);
            }
            backend.updateDates(pid, timestamp, db);
            db.setLatestKey(key);
            transaction.commit();
            log.info("objectStateChanged({},{},{}) Completed", pid, timestamp, newstate);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object created for pid='" + pid +
                                                    "' at date='" + timestamp.getTime() + "' and state='"+newstate+"'", e);
        }
    }

    @Override
    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) throws UpdateTrackerStorageException {
        DB db = dbfac.createReadonlyDBConnection();
        Transaction transaction = db.beginTransaction();
        try {
            log.info("lookup({},{},{},{},{},{}) Starting", since,viewAngle,offset,limit,state,collection);
            final List<Record> entries = backend.lookup(since, viewAngle, offset, limit, state, collection, db);
            log.debug("lookup({},{},{},{},{},{}) Completed", since, viewAngle, offset, limit, state, collection);
            return entries;
        } catch (HibernateException e) {
            throw new UpdateTrackerStorageException("Failed to query for since='"+since.getTime()+"', viewAngle='"+viewAngle+"', offset='"+offset+"', limit="+limit+"', state='"+state+"', collection='"+collection+"'", e);
        } finally {
            transaction.commit();
        }
    }

    @Override
    public Date lastChanged() throws UpdateTrackerStorageException {
        DB db = dbfac.createReadonlyDBConnection();
        Transaction transaction = db.beginTransaction();
        try {
            return backend.lastChanged(db);
        } catch (HibernateException e) {
            throw new UpdateTrackerStorageException("Failed to query for last changed object", e);
        } finally {
            transaction.commit();
        }
    }

    @Override
    public void close() {
        dbfac.close();
    }

    @Override
    public long getLatestKey() {
        DB db = dbfac.createReadonlyDBConnection();
        Transaction transaction = db.beginTransaction();
        try {
            return db.getLatestKey();
        } finally {
            transaction.commit();
        }
    }

    private class ObjectOfContentModelReconnector implements Callable<Set<Record>> {
        private final String object;
        private final Date timestamp;
        private final DBFactory dbfac;

        public ObjectOfContentModelReconnector(String object, Date timestamp, DBFactory dbfac) {
            this.object = object;
            this.timestamp = timestamp;
            this.dbfac = dbfac;
        }

        @Override
        public Set<Record> call() throws Exception {
            DB session = dbfac.createReadonlyDBConnection();
            //This is a threadlocal session. It will not see the uncommitted changes from the parent session
            //but there should be none at this point
            Transaction transaction = session.beginTransaction();
            Set<Record> changedRecords = null;
            try {
                Set<String> collections = fedora.getCollections(object, timestamp);
                State state = fedora.getState(object, timestamp);
                changedRecords = backend.recalculateRecordsBasedOnThisPid(object,
                                                                          timestamp,
                                                                          session,
                                                                          collections,
                                                                          state);
            } catch (FedoraFailedException | UpdateTrackerStorageException e) {
                transaction.rollback(); //I do not know if readonly transactions should be rolled back
                throw e;//yes, rethrow. I do not want to lose the type
            }
            transaction.commit();
            return changedRecords;
        }
    }

    public static <T> T getCompleted(String contentModel, Set<Future<T>> results,
                                     ExecutorCompletionService<T> progressTracker) throws
                                                                                   FedoraFailedException {
        T changedRecords;
        try {
            Future<T> future = progressTracker.take();
            changedRecords = future.get();
        } catch (ExecutionException e) {
            cancelPendingTasks(results);
            throw new FedoraFailedException("Failed to calculate the changes caused by a change to content model '" +
                                            contentModel + "'", e);
        } catch (InterruptedException e) {
            cancelPendingTasks(results);
            throw new RuntimeException("Interrupted while waiting for results for recalculation of objects of '" +
                                       contentModel + "'", e);
        }
        return changedRecords;
    }

    public static <T> void cancelPendingTasks(Set<Future<T>> results) {
        for (Future<T> result : results) {
            result.cancel(true);
        }
    }
}
