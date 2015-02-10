package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import java.lang.String;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Restrictions.ne;
import static org.hibernate.criterion.Restrictions.not;

public class UpdateTrackerBackend {
    private Fedora fedora;

    public UpdateTrackerBackend(Fedora fedora) {
        this.fedora = fedora;
    }

    /**
     * Modify the persistent storage regarding a change.
     *
     * @param pid     the pid of the object that was changed
     * @param date    the date of the change
     * @param state   the state of the entries that should be updated
     * @param session
     */
    void modifyState(String pid, Date date, String state, Session session) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {


        /*


        Otherwise (object became Inactive): Do nothing (updateTimestamps)
         */

        /* If the object was not previously known in OBJECTS and if the object is an Entry object
                Create row in ENTRIES and OBJECTS denoting this Record
         */
        List<String> entryForViewAngles = entryForViewAngles(pid, date);
        for (String entryForViewAngle : entryForViewAngles) {
            DomsObject thisObject = new DomsObject(pid, pid, entryForViewAngle);
            Object actual = session.get(DomsObject.class, thisObject);
            if (actual == null) {
                Entry thisEntry = new Entry(pid, entryForViewAngle, "I", date);
                session.saveOrUpdate(thisObject);
                session.saveOrUpdate(thisEntry);
            } else {

            }
        }

        //TODO for now we assume that the state is always a change from the current. This might not always be the case.
        switch (state) {
            case "A":
                /*
                    If this object became Active
                        For each Record (ENTRIES row) where this is entryPid,
                                addsert a row to ENTRIES with state Active
                 */
                List<Entry> entriesToMakeActive = listAndCast(session.createCriteria(Entry.class)
                                                                     .add(eq("entryPid", pid))
                                                                     .add(eq("state", "I")));
                ;
                for (Entry entry : entriesToMakeActive) {
                    session.saveOrUpdate(new Entry(entry.getEntryPid(), entry.getViewAngle(), "A", date));
                }
                break;
            case "D":
                /*
                If this Object became Deleted
                    If this object is an Entry object
                        Delete all rows with this entry pid from OBJECTS
                        Remove all rows with this entry pid and state=Active from ENTRIES
                        change state to Deleted for all rows with this entryPid and state=Inactive from ENTRIES and update timestamp to now.
                    Otherwise
                        Delete all rows with objectPid=this from OBJECTS
                        For each of these entryPid/viewAngle pair
                            remove old view from OBJECTS
                            recalculate view of entryPid/viewAngle
                            update OBJECTS
                */
                session.createQuery("delete DomsObject d where d.entryPid= :pid")
                       .setParameter("pid", pid)
                       .executeUpdate();
                session.createQuery("delete Entry e where e.entryPid= :pid and e.state='A'")
                       .setParameter("pid", pid)
                       .executeUpdate();
                session.createQuery("update Entry e set e.state='D',e.dateForChange=:date where e.entryPid= :pid and e.state='I'")
                       .setParameter("pid", pid)
                       .setParameter("date", date)
                       .executeUpdate();

                //find all the records containing this object

                _recalcView(pid, date, session);
                break;
        }
    }

    private List<String> entryForViewAngles(String pid, Date date) throws FedoraFailedException {
        //TODO this should be cached or actually in the database
        List<ViewInfo> viewInfo = fedora.getViewInfo(pid, date);
        return isEntryForViewAngles(viewInfo);
    }

    List<String> isEntryForViewAngles(List<ViewInfo> viewInfo) {
        return viewInfo.stream().filter(ViewInfo::isEntry).map(ViewInfo::getViewAngle).collect(Collectors.toList());
    }

    void recalculateView(String pid, Date date, Session session) throws
                                                                 FedoraFailedException,
                                                                 UpdateTrackerStorageException {

        /*
        If this Object has now become an Entry Object for any ViewAngle
            insert row in ENTRIES
            insert row in OBJECTS
        If this object has now ceased to be an Entry Object for any ViewAngle
            remove row from ENTRIES
            remove row from OBJECTS
        If this object is an Entry object for any viewAngle
            update collection field in ENTRIES

        So we now knows that the list of entryPid/viewAngle/state rows in ENTRIES is correct. The only way an object can enter or leave a view is if some objects in the view have their relations (or state) changed (this is not entirely true, but we disregard that). We can find all view that this object belongs to, by querying OBJECTS

        for each entryPid/viewAngle this object is part of (query OBJECTS)
            remove old view from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS
         */

        List<ViewInfo> viewInfo = fedora.getViewInfo(pid, date);
        final List<String> entryForViewAngles = isEntryForViewAngles(viewInfo);
        for (String viewAngle : entryForViewAngles) {
            if (session.createCriteria(Entry.class)
                       .add(eq("entryPid", pid))
                       .add(eq("viewAngle", viewAngle))
                       .list()
                       .isEmpty()) {
                DomsObject thisObject = new DomsObject(pid, pid, viewAngle);
                Entry thisEntry = new Entry(pid, viewAngle, "I", date);
                session.saveOrUpdate(thisObject);
                session.saveOrUpdate(thisEntry);
            }
        }
        List<Entry> entriesToRemove;
        if (entryForViewAngles.isEmpty()) {
            entriesToRemove = listAndCast(session.createCriteria(Entry.class)
                                                 .add(eq("entryPid", pid))
                                                 .add(ne("state", "D")));
        } else {
            entriesToRemove = listAndCast(session.createCriteria(Entry.class)
                                                 .add(eq("entryPid", pid))
                                                 .add(not(in("viewAngle", entryForViewAngles)))
                                                 .add(ne("state", "D")));
        }

        for (Entry entry : entriesToRemove) {
            entry.setDateForChange(date);
            entry.setState("D");
            session.saveOrUpdate(entry);
            session.createQuery("delete DomsObject d where d.entryPid=:entryPid and d.viewAngle=:viewAngle and d.objectPid!=d.entryPid")
                   .setParameter("entryPid", entry.getEntryPid())
                   .setParameter("viewAngle", entry.getViewAngle())
                   .executeUpdate();
        }

        if (!entryForViewAngles.isEmpty()) {
            Set<Collection> collections = fedora.getCollections(pid, date)
                                                .stream()
                                                .map(rel -> new Collection(rel,pid))
                                                .collect(Collectors.toSet());
            collections.forEach(session::saveOrUpdate);
            final List<Entry> entries = listAndCast(session.createCriteria(Entry.class).add(eq("entryPid", pid)));
            entries.stream().forEach(entry -> {
                session.saveOrUpdate(entry);
            });
        }

        _recalcView(pid, date, session);
    }

    private void _recalcView(String pid, Date date, Session session) throws FedoraFailedException {
        List<DomsObject> records = listAndCast(session.createQuery("from DomsObject d where d.objectPid= :pid")
                                                      .setParameter("pid", pid));
        for (DomsObject record : records) {
            ViewBundle bundle = fedora.calcViewBundle(record.getEntryPid(), record.getViewAngle(), date);
            for (String viewObject : bundle.getContained()) {
                final DomsObject object = new DomsObject(viewObject, record.getEntryPid(), record.getViewAngle());
                final Object o = session.get(DomsObject.class, object);
                if (o != null) {
                    session.save(o);
                } else {
                    session.save(object);
                }
            }
            session.createQuery("delete DomsObject d where d.entryPid= :pid and d.viewAngle= :viewAngle and d.objectPid not in :contained")
                   .setParameter("pid", record.getEntryPid())
                   .setParameter("viewAngle", record.getViewAngle())
                   .setParameterList("contained", bundle.getContained())
                   .executeUpdate();
        }
    }


    void updateTimestamps(String pid, Date date, Session session) {
        final Query query
                = session.createQuery("update Entry e set e.dateForChange= :date where exists ( from DomsObject o where o.objectPid= :pid and o.entryPid=e.entryPid and o.viewAngle=e.viewAngle ) and (e.state= 'I' or (e.state= 'A' and e.dateForChange>=(select e2.dateForChange from Entry e2 where e2.viewAngle=e.viewAngle and e2.entryPid=e.entryPid and e2.state='I')))");

        query.setParameter("pid", pid);
        query.setParameter("date", date);
        query.executeUpdate();

        /*
        For each entryPid/ViewAngle in OBJECTS(objectPid):
            for each record in ENTRIES(entryPid,viewAngle)
                switch record.state
                    Inactive: update timestamp
                    Deleted: do nothing
                    Active: if entryPid is currently Active: update timestamp
         */

    }

    public void contentModelDeleted(String pid, Date date, Session session) {
        session.createQuery("delete ContentModel c where c.cmPid=:pid").setParameter("pid", pid).executeUpdate();
    }

    public void contentModelViewChanged(String pid, Date date, Session session) throws FedoraFailedException {
        List<ViewInfo> contentModelViewInfo = fedora.getContentModelViewInfo(pid, date);
        for (ViewInfo viewInfo : contentModelViewInfo) {
            session.save(new ContentModel(pid, viewInfo.getViewAngle(), viewInfo.isEntry()));
        }
        session.createQuery("delete ContentModel cm where cm.cmPid=:pid and cm.viewAngle not in :viewAngleList")
               .setParameter("pid", pid)
               .setParameterList("viewAngleList",
                                        contentModelViewInfo.stream()
                                                            .map(ViewInfo::getViewAngle)
                                                            .collect(Collectors.toSet()))
               .executeUpdate();
    }


    boolean isContentModel(String pid, Session session) {
        return !session.createCriteria(ContentModel.class).add(Restrictions.eq("cmPid", pid)).list().isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> listAndCast(Criteria criteria) {
        return criteria.list();
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> listAndCast(Query query) {
        return query.list();
    }
}
