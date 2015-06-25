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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

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
    private static Logger contentModelChangedLogging = LoggerFactory.getLogger(UpdateTrackerPersistentStoreImpl.class.getCanonicalName()+".ContentModelChangedLogging");
    private final DBFactory dbfac;


    private FedoraForUpdateTracker fedora;
    private UpdateTrackerBackend backend;

    public UpdateTrackerPersistentStoreImpl(FedoraForUpdateTracker fedora, UpdateTrackerBackend backend,
                                            DBFactory dbfac) {
        this.fedora = fedora;
        this.backend = backend;
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
        log.debug("ObjectCreated({},{}) Starting", pid, timestamp);
        try {
            Set<String> collections = fedora.getCollections(pid, timestamp);
            State ingestState = fedora.getState(pid, timestamp);
            log.debug("Found collections {} and state {}  for pid {} at timestamp {}",ingestState,collections,pid,timestamp);
            for (String collection : collections) {
                backend.modifyState(pid, timestamp, collection, ingestState, db);
            }
            log.debug("Starting recalculation of records from pid {} for timestamp {}",pid,timestamp);
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
                    if (fedora.isCurrentlyContentModel(pid)) {
                        contentModelChangedLogging.warn("Content model {} changed, but records are not recalculated", pid);
                        fedora.invalidateContentModel(pid);
                    }
                    if (dsid.equals("RELS-EXT")) {
                        Set<String> collections = fedora.getCollections(pid, timestamp);
                        State state = fedora.getState(pid, timestamp);
                        log.debug("Found collections {} and state {} for pid {} at timestamp {}",collections,state,pid,timestamp);
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
            log.debug("Found collections {} for pid {} at timestamp {}",collections,pid,timestamp);
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
        log.info("lookup({},{},{},{},{},{}) Starting", since,viewAngle,offset,limit,state,collection);
        DB db = dbfac.createReadonlyDBConnection();
        Transaction transaction = db.beginTransaction();
        try {
            final List<Record> entries = backend.lookup(since, viewAngle, offset, limit, state, collection, db);
            log.debug("lookup({},{},{},{},{},{}) Completed, found {} records", since, viewAngle, offset, limit, state, collection,entries.size());
            return entries;
        } catch (HibernateException e) {
            throw new UpdateTrackerStorageException("Failed to query for since='"+since.getTime()+"', viewAngle='"+viewAngle+"', offset='"+offset+"', limit="+limit+"', state='"+state+"', collection='"+collection+"'", e);
        } finally {
            transaction.commit();
        }
    }

    @Override
    public Date lastChanged(String viewangle, String collection) throws UpdateTrackerStorageException {
        log.debug("LastChanged called with viewAngle {} and collection {}",viewangle,collection);
        DB db = dbfac.createReadonlyDBConnection();
        Transaction transaction = db.beginTransaction();
        try {
            Date result = backend.lastChanged(db, viewangle, collection);
            log.debug("LastChanged called with viewAngle {} and collection {} returns {}",viewangle,collection,result);
            return result;
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
}
