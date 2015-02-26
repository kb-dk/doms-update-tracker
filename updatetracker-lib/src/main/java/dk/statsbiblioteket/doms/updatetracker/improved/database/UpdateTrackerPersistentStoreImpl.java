package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State.DELETED;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State.INACTIVE;

public class UpdateTrackerPersistentStoreImpl implements UpdateTrackerPersistentStore, AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(UpdateTrackerPersistentStoreImpl.class);

    private SessionFactory sessionFactory;

    private Fedora fedora;
    private UpdateTrackerBackend backend;


    public UpdateTrackerPersistentStoreImpl(Fedora fedora) {
        this.fedora = fedora;
        // A SessionFactory is set up once for an application
        sessionFactory = new Configuration().addAnnotatedClass(DomsObject.class)
                                            .addAnnotatedClass(Record.class)
                                            .configure()
                                            .buildSessionFactory();
        backend = new UpdateTrackerBackend(fedora);
    }

    /**
     * The object  was created.
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
                backend.modifyState(pid, timestamp, collection, INACTIVE, session);
                backend.reconnectObjects(pid, timestamp, session);
            }
            backend.updateTimestamps(pid, timestamp, session);

            transaction.commit();
            log.info("ObjectCreated({},{}) Completed", pid, date);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }

    }

    /**
     * The object was deleted
     *
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
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }
    }


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
                if (fedora.isContentModel(pid)) {
                    if ((dsid.equals("VIEW") || dsid.equals("RELS-EXT"))) {
                        contentModelChanged(pid, timestamp, session);
                    }
                } else {
                    if (dsid.equals("RELS-EXT")) {
                        objectRelationsChanged(pid, date);
                    }
                }
            }
            backend.updateTimestamps(pid, timestamp, session);
            transaction.commit();
            log.info("DatastreamChanged({},{},{}) Completed", pid, date, dsid);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }
    }

    private void contentModelChanged(String pid, Timestamp timestamp, Session session) throws
                                                                                       FedoraFailedException,
                                                                                       UpdateTrackerStorageException {
        fedora.invalidateContentModel(pid);
        for (String object : fedora.getObjectsOfThisContentModel(pid)) {
            backend.reconnectObjects(object, timestamp, session);
            backend.updateTimestamps(object, timestamp, session);
        }
    }


    /**
     * The object's relations changed. This can cause a recalculation of the viewStructure.
     *
     * @param pid  the pid of the object that changed
     * @param date the date of the change
     */
    public void objectRelationsChanged(String pid, Date date) throws
                                                              UpdateTrackerStorageException,
                                                              FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("objectRelationsChanged({},{}) Starting", pid, date);
        try {
            Timestamp timestamp = new Timestamp(date.getTime());
            if (fedora.isContentModel(pid)) {
                contentModelChanged(pid, timestamp, session);
            } else {
                backend.reconnectObjects(pid, timestamp, session);
                backend.updateTimestamps(pid, timestamp, session);
            }
            transaction.commit();
            log.info("objectRelationsChanged({},{}) Completed", pid, date);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }
    }


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
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
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
            session.getTransaction().rollback();
            throw new UpdateTrackerStorageException("Failed to query for", e);
        } finally {
            session.close();
        }
    }



    /**
     * Clear the entire database. Dangerous operation
     * TODO Remove this
     */
    public void clear() {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();

        List results = session.createCriteria(DomsObject.class).list();
        for (Object result : results) {
            session.delete(result);
        }
        results = session.createCriteria(Record.class).list();
        for (Object result : results) {
            session.delete(result);
        }
        transaction.commit();
    }


    /**
     * Print the entire database.
     */
    public void dumpToStdOut() {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();

        List results = session.createCriteria(DomsObject.class).list();
        for (Object result : results) {
            System.out.println(result.toString());
        }
        results = session.createCriteria(Record.class).list();
        for (Object result : results) {
            System.out.println(result.toString());
        }
        transaction.commit();
    }

    @Override
    public void close() {
        sessionFactory.close();
    }
}
