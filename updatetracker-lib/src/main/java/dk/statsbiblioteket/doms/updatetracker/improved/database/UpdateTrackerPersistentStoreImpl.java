package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.hibernate.*;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.criterion.Order;

import java.util.*;

import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.ge;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/27/11
 * Time: 2:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackerPersistentStoreImpl implements UpdateTrackerPersistentStore, AutoCloseable {

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
        try {
            if (isNewContentModel(pid)){
                backend.contentModelViewChanged(pid,date,session);
            } else {
                backend.modifyState(pid, date, "I", session);
                backend.recalculateView(pid, date, session);
                backend.updateTimestamps(pid, date, session);
            }
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        } finally {
            if (!transaction.wasRolledBack()) {
                transaction.commit();
            }
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
        try {
            backend.modifyState(pid, date, "D", session);
            backend.updateTimestamps(pid, date, session);
            if (backend.isContentModel(pid,session)) {
                backend.contentModelDeleted(pid, date,session);
                for (String subscriber : getSubscribingObjects(pid)) {
                    backend.recalculateView(subscriber, date, session);
                    backend.updateTimestamps(subscriber, date, session);
                }
            }
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        } finally {
            if (!transaction.wasRolledBack()) {
                transaction.commit();
            }
        }
    }


    @Override
    public void datastreamChanged(String pid, Date date, String dsid) throws
                                                                      UpdateTrackerStorageException,
                                                                      FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        try {
            if (backend.isContentModel(pid,session)) {
                if (dsid != null && dsid.equals("VIEW")) {
                    backend.contentModelViewChanged(pid, date,session);
                    for (String subscriber : getSubscribingObjects(pid)) {
                        backend.recalculateView(subscriber, date, session);
                        backend.updateTimestamps(subscriber, date, session);
                    }
                }
            } else if (dsid != null && dsid.equals("RELS-EXT") && (backend.isContentModel(pid,session) || isNewContentModel(pid))) {
                    backend.contentModelViewChanged(pid,date,session);
                    for (String subscriber : getSubscribingObjects(pid)) {
                        backend.recalculateView(subscriber, date, session);
                        backend.updateTimestamps(subscriber, date, session);
                    }

            } else {
                if (dsid != null && dsid.equals("RELS-EXT")) {
                    objectRelationsChanged(pid, date);
                    return;
                }
                backend.updateTimestamps(pid, date, session);
            }
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        } finally {
            if (!transaction.wasRolledBack()) {
                transaction.commit();
            }
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
        try {
            backend.recalculateView(pid, date, session);
            backend.updateTimestamps(pid, date, session);
            if (backend.isContentModel(pid,session) || isNewContentModel(pid)) {
                backend.contentModelViewChanged(pid, date,session);
                for (String subscriber : getSubscribingObjects(pid)) {
                    backend.recalculateView(subscriber, date, session);
                    backend.updateTimestamps(subscriber, date, session);
                }
            }
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        } finally {
            if (!transaction.wasRolledBack()) {
                transaction.commit();
            }
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
        try {
            backend.modifyState(pid, date, newstate, session);
            backend.updateTimestamps(pid, date, session);
        } catch (Exception e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Hibernate Failed", e);
        } finally {
            if (!transaction.wasRolledBack()) {
                transaction.commit();
            }
        }
    }

    @Override
    public List<Entry> lookup(Date since, String viewAngle, int offset, int limit, String state,
                              boolean newestFirst) throws UpdateTrackerStorageException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        try {

            Criteria thing = session.createCriteria(Entry.class)
                                    .add(ge("dateForChange", since))
                                    .add(eq("viewAngle", viewAngle))
                                    .setFirstResult(offset)
                                    .setMaxResults(limit);

            if (newestFirst) {
                thing = thing.addOrder(Order.desc("dateForChange"));
            } else {
                thing = thing.addOrder(Order.asc("dateForChange"));
            }

            if (state != null && !state.trim().isEmpty()) {
                thing = thing.add(eq("state", state));
            }
            List<Entry> results = listAndCast(thing);


            List<Entry> entries = new ArrayList<Entry>(results.size());
            for (Entry result1 : results) {
                entries.add(result1);
            }

            transaction.commit();
            return entries;
        } catch (HibernateException e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Failed to query for", e);
        }
    }


    @SuppressWarnings("unchecked")
    private static <T> List<T> listAndCast(Criteria criteria) {
        return criteria.list();
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
