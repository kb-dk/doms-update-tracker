package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State.DELETED;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State.INACTIVE;

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

    private SessionFactory sessionFactory;

    private FedoraForUpdateTracker fedora;
    private UpdateTrackerBackend backend;


    public UpdateTrackerPersistentStoreImpl(File configFile, File hibernateMappings, FedoraForUpdateTracker fedora,
                                            UpdateTrackerBackend backend) {
        this.fedora = fedora;
        this.backend = backend;
        // A SessionFactory is set up once for an application
        final Configuration configuration = new Configuration()
                                                .configure(configFile);
        if (hibernateMappings != null) {
            configuration.addFile(hibernateMappings);
        }
        configuration.setInterceptor(new LastModifiableInterceptor());
        sessionFactory = configuration.buildSessionFactory();

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
        Session session = getSession();
        Transaction transaction = session.beginTransaction();
        log.debug("ObjectCreated({},{}) Starting",pid,timestamp);
        try {
            Set<String> collections = fedora.getCollections(pid, timestamp);
            State ingestState = fedora.getState(pid, timestamp);
            for (String collection : collections) {
                backend.modifyState(pid, timestamp, collection, ingestState, session);
            }
            backend.reconnectObjects(pid, timestamp, session, collections);
            backend.updateDates(pid, timestamp, session);

            setLatestKey(key, session);
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

    private Session getSession() {
        Session session = sessionFactory.getCurrentSession();
        session.setFlushMode(FlushMode.COMMIT);
        return session;
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
        Session session = getSession();
        Transaction transaction = session.beginTransaction();
        log.debug("ObjectDeleted({},{}) Starting", pid, timestamp);
        try {
            backend.modifyState(pid, timestamp, null, DELETED, session);
            backend.updateDates(pid, timestamp, session);
            setLatestKey(key, session);
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
     * @param pid
     * @param timestamp
     * @param dsid
     * @param key the key from the work log table, that defined this operation.
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void datastreamChanged(String pid, Date timestamp, String dsid, long key) throws
                                                                      UpdateTrackerStorageException,
                                                                      FedoraFailedException {
        Session session = getSession();
        Transaction transaction = session.beginTransaction();
        log.debug("DatastreamChanged({},{},{}) Starting", pid, timestamp,dsid);
        try {
            if (dsid != null) {
                if ((dsid.equals("VIEW") || dsid.equals("RELS-EXT"))) {
                    if (fedora.isCurrentlyContentModel(pid, timestamp)) {
                        fedora.invalidateContentModel(pid);
                        int i = 0;
                        //TODO this set can be HUGE
                        final Set<String> objectsOfThisContentModel = fedora.getObjectsOfThisContentModel(pid);
                        for (String object : objectsOfThisContentModel) {
                            log.debug("Working on object {}, number {} of the {} objects of content model {}",object,i++,objectsOfThisContentModel.size(),pid);
                            Set<String> collections = fedora.getCollections(object, timestamp);
                            backend.reconnectObjects(object, timestamp, session, collections);
                            backend.updateDates(object, timestamp, session);
                            //TODO is flush the right thing to clear the session here? No need to keep track of the objects from last iteration of this loop
                            session.flush();
                        }
                    } else if (dsid.equals("RELS-EXT")) {
                        Set<String> collections = fedora.getCollections(pid, timestamp);
                        backend.reconnectObjects(pid, timestamp, session, collections);
                    }
                }
            }
            backend.updateDates(pid, timestamp, session);
            setLatestKey(key, session);
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
     * @param pid
     * @param timestamp
     * @param newstate
     * @param key the key from the work log table, that defined this operation.
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void objectStateChanged(String pid, Date timestamp, String newstate, long key) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {

        Session session = getSession();
        Transaction transaction = session.beginTransaction();
        log.debug("objectStateChanged({},{},{}) Starting", pid, timestamp, newstate);
        try {
            Set<String> collections = fedora.getCollections(pid, timestamp);
            for (String collection : collections) {
                backend.modifyState(pid, timestamp, collection, State.fromName(newstate), session);
            }
            backend.updateDates(pid, timestamp, session);
            setLatestKey(key, session);
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
        StatelessSession session = sessionFactory.openStatelessSession();
        session.beginTransaction();
        try {
            log.info("lookup({},{},{},{},{},{}) Starting", since,viewAngle,offset,limit,state,collection);


            final List<Record> entries = backend.lookup(since, viewAngle, offset, limit, state, collection, session);
            session.getTransaction().commit();
            log.debug("lookup({},{},{},{},{},{}) Completed", since, viewAngle, offset, limit, state, collection);
            return entries;
        } catch (HibernateException e) {
            try {
                session.getTransaction()
                       .rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Failed to query for since='"+since.getTime()+"', viewAngle='"+viewAngle+"', offset='"+offset+"', limit="+limit+"', state='"+state+"', collection='"+collection+"'", e);
        } finally {
            session.close();
        }
    }

    @Override
    public Date lastChanged() throws UpdateTrackerStorageException {
        StatelessSession session = sessionFactory.openStatelessSession();
        session.beginTransaction();
        try {
            final Date lastChangeRecord = backend.lastChanged(session);
            session.getTransaction()
                   .commit();
            return lastChangeRecord;
        } catch (HibernateException e) {
            try {
                session.getTransaction()
                       .rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Failed to query for last changed object", e);
        } finally {
            session.close();
        }
    }

    @Override
    public void close() {
        sessionFactory.close();
    }

    @Override
    public long getLatestKey() {
        StatelessSession session = sessionFactory.openStatelessSession();
        try {
            List<LatestKey> list = session.createCriteria(LatestKey.class).list();
            if (list.size() > 0) {
                return list.get(0).getKey();
            } else {
                return 0L;
            }
        } finally {
            session.close();
        }
    }

    private void setLatestKey(Long latestKey, Session session) throws UpdateTrackerStorageException {
        session.saveOrUpdate(new LatestKey(latestKey));
    }
}
