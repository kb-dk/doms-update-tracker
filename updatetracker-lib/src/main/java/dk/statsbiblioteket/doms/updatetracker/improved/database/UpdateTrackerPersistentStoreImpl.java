package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
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
import java.sql.Timestamp;
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


    public UpdateTrackerPersistentStoreImpl(File configFile, FedoraForUpdateTracker fedora,
                                            UpdateTrackerBackend backend) {
        this.fedora = fedora;
        this.backend = backend;
        // A SessionFactory is set up once for an application
        sessionFactory = new Configuration().addAnnotatedClass(DomsObject.class)
                                            .addAnnotatedClass(Record.class)
                                            .configure(configFile)
                                            .buildSessionFactory();

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
     * @param pid  the pid of the new object
     * @param date the date of the object creation
     */
    @Override
    public void objectCreated(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("ObjectCreated({},{}) Starting",pid,date);
        try {
            Set<String> collections = fedora.getCollections(pid, date);
            Timestamp timestamp = new Timestamp(date.getTime());
            for (String collection : collections) {
                State ingestState = fedora.getState(pid,date);
                backend.modifyState(pid, timestamp, collection, ingestState, session);
                backend.reconnectObjects(pid, timestamp, session);
            }
            backend.updateTimestamps(pid, timestamp, session);

            transaction.commit();
            log.info("ObjectCreated({},{}) Completed", pid, date);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he){
                log.error("Failed to rollback transaction",he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object created for pid='" + pid +
                                                    "' at date='" + date.getTime() + "'", e);
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
     *           updateTimestamps()
     *           if content model
     *              for all objects of this class
     *                  reconnectObjects()
     *                  updateTimestamp()
     *
     *
     *  If you purge a content model, that people still link to, you are breaking the link structure, and I feel the
     *  update tracker is justified in ignoring it. If nobody links to the content model, it does not matter that you
     *  purged it.
     If you just changed the state to Deleted, it does, per definition, not matter
      TODO update the wiki page

     * @param pid  the pid of the object
     * @param date the date of the change
     */
    @Override
    public void objectDeleted(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("ObjectDeleted({},{}) Starting", pid, date);
        try {
            Timestamp timestamp = new Timestamp(date.getTime());
            backend.modifyState(pid, timestamp, null, DELETED, session);
            backend.updateTimestamps(pid, timestamp, session);
            transaction.commit();
            log.info("ObjectDeleted({},{}) Completed", pid, date);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object deleted for pid='" + pid +
                                                    "' at date='" + date.getTime() + "'", e);
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
     *           - updateTimestamp
     *      Action:
     *           if RELS-EXT
     *                reconnectObjects(this)
     *           fi
     *           updateTimestamp(this)
     *           if VIEW and Content Model
     *                for all objects of this class
     *                     reconnectObjects(object)
     *                     updateTimestamp(object)
     fi
     * @param pid
     * @param date
     * @param dsid
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void datastreamChanged(String pid, Date date, String dsid) throws
                                                                      UpdateTrackerStorageException,
                                                                      FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("DatastreamChanged({},{},{}) Starting", pid, date,dsid);
        try {
            Timestamp timestamp = new Timestamp(date.getTime());
            if (dsid != null) {
                if ((dsid.equals("VIEW") || dsid.equals("RELS-EXT"))) {
                    if (fedora.isCurrentlyContentModel(pid, date)) {
                        fedora.invalidateContentModel(pid);
                        for (String object : fedora.getObjectsOfThisContentModel(pid)) {
                            backend.reconnectObjects(object, timestamp, session);
                            backend.updateTimestamps(object, timestamp, session);
                        }
                    } else if (dsid.equals("RELS-EXT")) {
                        backend.reconnectObjects(pid, timestamp, session);
                    }
                }
            }
            backend.updateTimestamps(pid, timestamp, session);
            transaction.commit();
            log.info("DatastreamChanged({},{},{}) Completed", pid, date, dsid);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in datastream changed for pid='" + pid +
                                                    "' at date='" + date.getTime() + "' and dsid='"+dsid+"'", e);
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
     *           updateTimestamp(this)
     *           if this is a content model
     *                for all objects of this class
     *                     reconnectObjects(object of this class)
     *                     updateTimestamp(object of this class)
     *
     * @param pid  the pid of the object that changed
     * @param date the date of the change
     */
    public void objectRelationsChanged(String pid, Date date) throws
                                                              UpdateTrackerStorageException,
                                                              FedoraFailedException {
        datastreamChanged(pid,date,"RELS-EXT");
    }


    /**
     * Object State Changed: The Object changed state in DOMS
     *      Fedora operations:
     *           - modifyObject
     *       Action:
     *            modifyState(state)
     *            updateTimestamp()
     *
     * @param pid
     * @param date
     * @param newstate
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Override
    public void objectStateChanged(String pid, Date date, String newstate) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {

        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("objectStateChanged({},{},{}) Starting", pid, date,newstate);
        try {
            Timestamp timestamp = new Timestamp(date.getTime());
            backend.modifyState(pid, timestamp, null, State.fromName(newstate), session);
            backend.updateTimestamps(pid, timestamp, session);
            transaction.commit();
            log.info("objectStateChanged({},{},{}) Completed", pid, date, newstate);
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (HibernateException he) {
                log.error("Failed to rollback transaction", he);
            }
            throw new UpdateTrackerStorageException("Hibernate Failed in object created for pid='" + pid +
                                                    "' at date='" + date.getTime() + "' and state='"+newstate+"'", e);
        }
    }

    @Override
    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) throws UpdateTrackerStorageException {
        StatelessSession session = sessionFactory.openStatelessSession();
        session.beginTransaction();
        try {
            log.info("lookup({},{},{},{},{},{}) Starting", since,viewAngle,offset,limit,state,collection);


            final List<Record> entries = backend.lookup(new Timestamp(since.getTime()),
                                                        viewAngle,
                                                        offset,
                                                        limit,
                                                        state,
                                                        collection,
                                                        session);
            session.getTransaction().commit();
            log.info("lookup({},{},{},{},{},{}) Completed", since, viewAngle, offset, limit, state, collection);
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
}
