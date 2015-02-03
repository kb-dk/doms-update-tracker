package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import org.hibernate.*;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.criterion.NaturalIdentifier;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/27/11
 * Time: 2:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class DomsUpdateTrackerUpdateTrackerPersistentStoreImpl implements UpdateTrackerPersistentStore {

    public static final String OBJECT_PID = "objectPid";
    public static final String STATE_DELETED = "D";
    public static final String STATE_PUBLISHED = "A";
    public static final String STATE_INPROGRESS = "I";
    public static final String ENTRY_PID = "entryPid";
    public static final String VIEW_ANGLE = "viewAngle";
    public static final String STATE = "state";
    public static final String DATE_FOR_CHANGE = "dateForChange";
    private SessionFactory sessionFactory;

    Fedora fedora;

    public DomsUpdateTrackerUpdateTrackerPersistentStoreImpl(Fedora fedora) {
        this.fedora = fedora;
    }

    public void setUp() throws Exception {
        // A SessionFactory is set up once for an application
        sessionFactory = new AnnotationConfiguration()
                                 .addAnnotatedClass(DomsObject.class)
                                 .addAnnotatedClass(Entry.class)
                                 .configure()
                                 .buildSessionFactory();
    }


    /**
     * The object  was created.
     *
     * @param pid  the pid of the new object
     * @param date the date of the object creation
     */
    @Override
    public void objectCreated(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        //Modify the persistent storage, changing the entry for the Inprogress Entry
        objectModified(pid, date, STATE_INPROGRESS);
        objectRelationsChanged(pid, date);
    }

    /**
     * The object was deleted
     *
     * @param pid  the pid of the object
     * @param date the date of the change
     */
    @Override
    public void objectDeleted(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        //Modify the persistent storage, changing the entry for the Deleted Entry
        objectModified(pid, date, STATE_DELETED);
    }

    /**
     * The object was published
     *
     * @param pid  the pid of the object
     * @param date the date of the change
     */
    @Override
    public void objectPublished(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        //Modify the persistent storage, changing the entry for the Deleted Entry
        objectModified(pid, date, STATE_PUBLISHED);
    }

    /**
     * Any other kind of changes
     *
     * @param pid  the pid of the object
     * @param date the date of the change
     */
    @Override
    public void objectChanged(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException {
        objectModified(pid, date, STATE_INPROGRESS);
    }


    /**
     * Modify the persistent storage regarding a change.
     *
     * @param pid   the pid of the object that was changed
     * @param date  the date of the change
     * @param state the state of the entries that should be updated
     */
    private void objectModified(String pid, Date date, String state) throws UpdateTrackerStorageException, FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();

        try {
            //Find the DomsObject rows that regard this object.
            //There will be one per entry/viewAngle combination
            final Criteria criteria = session.createCriteria(DomsObject.class)
                                             .add(Restrictions.naturalId().set(OBJECT_PID, pid));
            List<DomsObject> results = listAndCast(criteria);

            //Find all Entries that include this object
            for (DomsObject result : results) {
                if (!result.getEntryPid().equals(pid)) {
                    //Mark them as updated
                    updateEntry(session,
                                       result.getEntryPid(),
                                       state,
                                       result.getViewAngle(),
                                       date);
                }
            }


            // Find view Info for this object
            List<ViewInfo> viewInfoList = fedora.getViewInfo(pid, date);
            for (ViewInfo viewInfo : viewInfoList) {
                //If it is an entry object, set it in the ENTRIES table
                if (viewInfo.isEntry()) {
                    updateEntry(session, pid, state, viewInfo.getViewAngle(), date);
                    updateDomsObjects(session, pid, pid, viewInfo.getViewAngle());
                }
            }
            transaction.commit();

        } catch (HibernateException e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Failed to commit transaction for pid='" + pid + "', state='" + state + "' and date='" + date.toString() + "'", e);
        } catch (FedoraFailedException e) {
            transaction.rollback();
            throw new FedoraFailedException("Rethrowing exception after rolling back", e);
        }


    }

    @SuppressWarnings("unchecked")
    public static <T> List<T>  listAndCast(Criteria criteria) {
        return criteria.list();
    }

    /**
     * Update the Entries table regarding a change
     *
     * @param session   the database session
     * @param entryPid  the pid of the Entry object
     * @param state     the state of the Entry row
     * @param viewAngle the viewAngle of the Entry
     * @param date      the date of the Change
     */
    private void updateEntry(Session session, String entryPid, String state, String viewAngle, Date date) {
        NaturalIdentifier restrictions = Restrictions
                                                 .naturalId();


        //Set all the parameters that have been included as restrictions
        restrictions = restrictions.set(ENTRY_PID, entryPid);
        restrictions = restrictions.set(STATE, state);
        restrictions = restrictions.set(VIEW_ANGLE, viewAngle);

        //Find the Entry objects that fulfill these restrictions
        List<Entry> results = listAndCast(session.createCriteria(Entry.class).add(restrictions));

        //There might be no Entry, but if we are here, we know that an entry should exist, so create it.
        if (results.size() == 0) {
            session.save(new Entry(entryPid, viewAngle, state, date));
        } else {
            for (Entry result : results) {

                //Or there might have been some results

                //Is this entry older than the current change?
                if (result.getDateForChange().getTime() < date.getTime()) {
                    result.setDateForChange(date);

                    result.setEntryPid(entryPid);
                    result.setState(state);
                    result.setViewAngle(viewAngle);
                }
                //Save the entry
                session.save(result);
            }
        }
    }


    /**
     * update the Objects table with information about this object
     *
     * @param session   the database session
     * @param objectPid the pid of the Object
     * @param entryPid  the pid of the Entry that reference this object
     * @param viewAngle the viewAngle of the entry that reference this object
     */
    private void updateDomsObjects(Session session, String objectPid, String entryPid, String viewAngle) {
        List results = session.createCriteria(DomsObject.class).add(Restrictions.naturalId()
                                                                                .set(OBJECT_PID, objectPid)
                                                                                .set(ENTRY_PID, entryPid)
                                                                                .set(VIEW_ANGLE, viewAngle))
                              .list();
        //TODO: can we avoid the query and just save each time?
        if (results.size() == 0) {
            session.save(new DomsObject(objectPid, entryPid, viewAngle));
        }

    }


    private void removeFromEntries(Session session, String entryPid, String state, String viewAngle) {
        List<Entry> results = listAndCast(session.createCriteria(Entry.class)
                                                 .add(Restrictions
                                                              .naturalId()
                                                              .set(ENTRY_PID, entryPid)
                                                              .set(STATE, state)
                                                              .set(VIEW_ANGLE, viewAngle)));
        for (Entry result : results) {
            session.delete(result);

        }

    }

    private void removeNotListedFromDomsObjects(Session session, List<String> objectPid, String entryPid, String viewAngle) {

        List<DomsObject> results;
        NaturalIdentifier query = Restrictions.naturalId()
                                              .set(ENTRY_PID, entryPid)
                                              .set(VIEW_ANGLE, viewAngle);

        results = listAndCast(session.createCriteria(DomsObject.class).add(query));

        for (DomsObject result1 : results) {
            if (!objectPid.contains(result1.getObjectPid())) {
                session.delete(result1);
            }

        }

    }


    /**
     * The object's relations changed. This can cause a recalculation of the viewStructure.
     *
     * @param pid  the pid of the object that changed
     * @param date the date of the change
     */
    public void objectRelationsChanged(String pid, Date date)
            throws UpdateTrackerStorageException, FedoraFailedException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();


        //This can change the structure of the views and we must therefore recaculate the views

        //if a current entry object use this object, we will need to recalculate the view of that object

        //This method will only be called on objects that already exist. Objects cannot change type once created.
        //ObjectModified() creates an Entry row, if the object is an entry object. As such, there will always be
        // the correct Entry entries when this method is called, and these should just be recalculated

        List<DomsObject> results = session.createCriteria(DomsObject.class)
                                          .add(Restrictions
                                                       .naturalId()
                                                       .set(OBJECT_PID, pid)
                                              )
                                          .list();

        //we now have a list of all the entries that include this object.


        for (DomsObject result : results) {

            //get the ViewBundle from fedora
            ViewBundle bundle = fedora.calcViewBundle(result.getEntryPid(), result.getViewAngle(), date);


            //First, remove all the objects in this bundle from the table
            removeNotListedFromDomsObjects(session, bundle.getContained(), result.getEntryPid(), result.getViewAngle());

            //Add all the objects from the bundle to the objects Table.
            for (String objectPid : bundle.getContained()) {
                updateDomsObjects(session, objectPid, bundle.getEntry(), bundle.getViewAngle());
            }

            updateEntry(session, bundle.getEntry(), STATE_INPROGRESS, bundle.getViewAngle(), date);

        }
        try {
            transaction.commit();
        } catch (HibernateException e) {
            transaction.rollback();
            throw new UpdateTrackerStorageException("Failed to commit transaction for pid='" + pid + "' and date='" + date.toString() + "'", e);
        }
    }


    @Override
    public List<Entry> lookup(Date since, String viewAngle, int offset, int limit, String state, boolean newestFirst) throws UpdateTrackerStorageException {
        Session session = sessionFactory.getCurrentSession();
        Transaction transaction = session.beginTransaction();
        try {

            Criteria thing = session.createCriteria(Entry.class)
                                    .add(Restrictions.ge(DATE_FOR_CHANGE, since))
                                    .add(Restrictions.naturalId().set(VIEW_ANGLE, viewAngle))
                                    .setFirstResult(offset)
                                    .setMaxResults(limit);

            if (newestFirst) {
                thing = thing.addOrder(Order.desc(DATE_FOR_CHANGE));
            } else {
                thing = thing.addOrder(Order.asc(DATE_FOR_CHANGE));
            }

            if (state != null && !state.trim().isEmpty()) {
                thing = thing.add(Restrictions.naturalId().set(STATE, state));
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


    /**
     * Clear the entire database. Dangerous operation
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
     * Clear the entire database. Dangerous operation
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

}
