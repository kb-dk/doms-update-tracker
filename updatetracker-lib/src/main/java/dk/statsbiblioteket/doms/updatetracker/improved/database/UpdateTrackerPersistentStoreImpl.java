package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.cfg.AnnotationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.HibernateUtils.listAndCast;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/27/11
 * Time: 2:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackerPersistentStoreImpl implements UpdateTrackerPersistentStore, AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(UpdateTrackerPersistentStoreImpl.class);

    private SessionFactory sessionFactory;

    private Fedora fedora;
    private UpdateTrackerBackend backend;

    public UpdateTrackerPersistentStoreImpl(Fedora fedora) {
        this.fedora = fedora;
    }

    public void setUp() throws Exception {
        // A SessionFactory is set up once for an application
        sessionFactory = new AnnotationConfiguration().addAnnotatedClass(DomsObject.class)
                                                      .addAnnotatedClass(Entry.class)
                                                      .addAnnotatedClass(Collection.class)
                                                      .addAnnotatedClass(ContentModel.class)
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
            if (isNewContentModel(pid)){
                backend.contentModelViewChanged(pid,date,session);
            } else {
                backend.modifyState(pid, date, "I", session);
                backend.modifyRelations(pid, date, session);
                backend.updateTimestamps(pid, date, session);
            }
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
            backend.modifyState(pid, date, "D", session);
            backend.updateTimestamps(pid, date, session);
            if (backend.isContentModel(pid,session)) {
                backend.contentModelDeleted(pid, date,session);
                for (String subscriber : getSubscribingObjects(pid)) {
                    backend.modifyRelations(subscriber, date, session);
                    backend.updateTimestamps(subscriber, date, session);
                }
            }
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
            if (backend.isContentModel(pid,session)) {
                if (dsid != null && dsid.equals("VIEW")) {
                    backend.contentModelViewChanged(pid, date,session);
                    for (String subscriber : getSubscribingObjects(pid)) {
                        backend.modifyRelations(subscriber, date, session);
                        backend.updateTimestamps(subscriber, date, session);
                    }
                }
            } else if (dsid != null && dsid.equals("RELS-EXT") && (backend.isContentModel(pid,session) || isNewContentModel(pid))) {
                    backend.contentModelViewChanged(pid,date,session);
                    for (String subscriber : getSubscribingObjects(pid)) {
                        backend.modifyRelations(subscriber, date, session);
                        backend.updateTimestamps(subscriber, date, session);
                    }

            } else {
                if (dsid != null && dsid.equals("RELS-EXT")) {
                    objectRelationsChanged(pid, date);
                    return;
                }
                backend.updateTimestamps(pid, date, session);
            }
            transaction.commit();
            log.info("DatastreamChanged({},{},{}) Completed", pid, date, dsid);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
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
            backend.modifyRelations(pid, date, session);
            backend.updateTimestamps(pid, date, session);
            if (backend.isContentModel(pid,session) || isNewContentModel(pid)) {
                backend.contentModelViewChanged(pid, date,session);
                for (String subscriber : getSubscribingObjects(pid)) {
                    backend.modifyRelations(subscriber, date, session);
                    backend.updateTimestamps(subscriber, date, session);
                }
            }
            transaction.commit();
            log.info("objectRelationsChanged({},{}) Completed", pid, date);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }
    }

    //TODO this checks if the pid is a content model in fedora
    private boolean isNewContentModel(String pid) {
        return false;
    }

    @Override
    public void objectStateChanged(String pid, Date date, String newstate) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {

        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        log.info("objectStateChanged({},{},{}) Starting", pid, date,newstate);
        try {
            backend.modifyState(pid, date, newstate, session);
            backend.updateTimestamps(pid, date, session);
            transaction.commit();
            log.info("objectStateChanged({},{},{}) Completed", pid, date, newstate);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        }
    }

    @Override
    public List<Entry> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) throws UpdateTrackerStorageException {
        StatelessSession session = sessionFactory.openStatelessSession();
        session.beginTransaction();
        try {
            log.info("lookup({},{},{},{},{},{}) Starting", since,viewAngle,offset,limit,state,collection);

            Query query
                    = session.createQuery("from Entry e where e.dateForChange>=:since and e.viewAngle=:viewAngle and e.state in :stateList and e.entryPid in (select col.entryPid from Collection col where col.collectionID=:collection) order by e.dateForChange asc ");
            query.setReadOnly(true);
            query.setFirstResult(offset).setMaxResults(limit);
            query.setParameter("since",since).setParameter("collection",collection).setParameter("viewAngle",viewAngle);
            if (state != null && !state.trim().isEmpty()) {
                query.setParameterList("stateList", Arrays.asList(state));
            } else {
                query.setParameterList("stateList", Arrays.asList("A","I","D"));
            }

            final List<Entry> entries = listAndCast(query);
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
        results = session.createCriteria(Entry.class).list();
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
        results = session.createCriteria(Entry.class).list();
        for (Object result : results) {
            System.out.println(result.toString());
        }
        transaction.commit();
    }

    @Override
    public void close() {
        sessionFactory.close();
    }


    private String[] getSubscribingObjects(String pid) {
        //TODO
        return new String[0];
    }
}
