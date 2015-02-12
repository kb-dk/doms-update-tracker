package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.String;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.HibernateUtils.listAndCast;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Restrictions.ne;
import static org.hibernate.criterion.Restrictions.not;

public class UpdateTrackerBackend {
    private Fedora fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);


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

        log.debug("Starting modifyState({},{},{})",pid,date,state);

        /*


        Otherwise (object became Inactive): Do nothing (updateTimestamps)
         */

        /* If the object was not previously known in OBJECTS and if the object is an Entry object
                Create row in ENTRIES and OBJECTS denoting this Record
         */
        List<String> entryForViewAngles = entryForViewAngles(pid, date);
        for (String entryForViewAngle : entryForViewAngles) {
            log.debug("Pid {} is an entry for viewangle {}",pid,entryForViewAngle);
            DomsObject thisObject = new DomsObject(pid, pid, entryForViewAngle);
            Object actual = session.get(DomsObject.class, thisObject);
            if (actual == null) {
                log.debug("Pid {} is not registered as an entry for viewAngle {}. Fixing",pid,entryForViewAngle);
                session.persist(thisObject);
                session.persist(new Entry(pid, entryForViewAngle, "I", date));
            }
        }

        //TODO for now we assume that the state is always a change from the current. This might not always be the case.
        switch (state) {
            case "A":
                log.debug("Switching on states for pid {}, got the Active branch",pid);
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
                log.debug("Switching on states for pid {}, got the Deleted branch", pid);
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

                recalculateView(pid, date, session);
                break;
            case "I":
                log.debug("Switching on states for pid {}, got the Inactive branch", pid);
                log.debug("Updating dateForChange to {} for entry {}",date,pid);
                session.createQuery("update Entry e set e.dateForChange=:date where e.entryPid= :pid and e.state='I'")
                       .setParameter("pid", pid)
                       .setParameter("date", date)
                       .executeUpdate();
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

    void modifyRelations(String pid, Date date, Session session) throws
                                                                 FedoraFailedException,
                                                                 UpdateTrackerStorageException {
        log.debug("starting modifyRelations({},{})",pid,date);

        List<ViewInfo> viewInfo = fedora.getViewInfo(pid, date);
        final List<String> entryForViewAngles = isEntryForViewAngles(viewInfo);

        addNewViewAngles(pid, date, session, entryForViewAngles);
        removePreviousViewAngles(pid, date, session, entryForViewAngles);
        updateCollections(pid, date, session, entryForViewAngles);
        recalculateView(pid, date, session);
    }

    private void addNewViewAngles(String pid, Date date, Session session, List<String> entryForViewAngles) {
        log.debug("Checking if {} have become an entry",pid);
        /*
        If this Object has now become an Entry Object for any ViewAngle
            insert row in ENTRIES
            insert row in OBJECTS
         */

        for (String viewAngle : entryForViewAngles) {
            if (session.createCriteria(Entry.class)
                       .add(eq("entryPid", pid))
                       .add(eq("viewAngle", viewAngle))
                       .add(eq("state", "I"))
                       .list()
                       .isEmpty()) {
                log.debug("{} is an entry for {} was not marked as such so update the database", pid,viewAngle);
                DomsObject thisObject = new DomsObject(pid, pid, viewAngle);
                Entry thisEntry = new Entry(pid, viewAngle, "I", date);
                session.persist(thisObject);
                session.persist(thisEntry);
            }
        }
    }

    private void removePreviousViewAngles(String pid, Date date, Session session, List<String> entryForViewAngles) {
    /*
    If this object has now ceased to be an Entry Object for any ViewAngle
        create new rom in ENTRIES with state D
        remove row from OBJECTS
     */
        log.debug("Checking if {} have ceased to be an entry",pid);
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
        if (!entriesToRemove.isEmpty()) {
            log.debug("Found that {} was entry marked as entry for these viewAngles {}, which should be removed",
                             pid,
                             entriesToRemove.stream().map(Entry::getViewAngle).distinct().collect(Collectors.toList()));
        }

        for (Entry entry : entriesToRemove) {
            log.debug("Removing entry {}", entry);
            entry.setDateForChange(date);
            entry.setState("D");
            session.saveOrUpdate(entry);
            log.debug("Removing all objects from the Record {}",entry);
            session.createQuery("delete DomsObject d where d.entryPid=:entryPid and d.viewAngle=:viewAngle and d.objectPid!=d.entryPid")
                   .setParameter("entryPid", entry.getEntryPid())
                   .setParameter("viewAngle", entry.getViewAngle())
                   .executeUpdate();
        }
    }

    private void updateCollections(String pid, Date date, Session session, List<String> entryForViewAngles) throws
                                                                                                            FedoraFailedException {
    /*
    If this object is an Entry object for any viewAngle
        update collection field in ENTRIES
     */
        if (!entryForViewAngles.isEmpty()) {
            log.debug("Updating collection information for pid {}",pid);
            Set<Collection> collections = fedora.getCollections(pid, date)
                                                .stream()
                                                .map(rel -> new Collection(rel,pid))
                                                .collect(Collectors.toSet());
            collections.forEach((object) -> {session.saveOrUpdate(object);log.debug("Entry {} is in collection {}",object.getEntryPid(),object.getCollectionID());});
        }
    }

    private void recalculateView(String pid, Date date, Session session) throws FedoraFailedException {
        log.debug("Recalculating view for {}",pid);
        /*
        So we now knows that the list of entryPid/viewAngle/state rows in ENTRIES is correct. The only way an object can enter or leave a view is if some objects in the view have their relations (or state) changed (this is not entirely true, but we disregard that). We can find all view that this object belongs to, by querying OBJECTS

        for each entryPid/viewAngle this object is part of (query OBJECTS)
            remove old view from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS

         */

        List<DomsObject> records = listAndCast(session.createQuery("from DomsObject d where d.objectPid= :pid")
                                                      .setParameter("pid", pid));
        log.debug("Find all records {} containing {} ",records,pid);
        for (DomsObject record : records) {
            final String entryPid = record.getEntryPid();
            final String viewAngle = record.getViewAngle();
            log.debug("Calculating view bundle for entry {} for viewAngle {} for date {}",entryPid,viewAngle,date );
            ViewBundle bundle = fedora.calcViewBundle(entryPid, viewAngle, date);
            for (String viewObject : bundle.getContained()) {
                log.debug("Marking object {} as part of record {},{},{}",viewObject,entryPid,viewAngle,date);
                final DomsObject object = new DomsObject(viewObject, entryPid, viewAngle);
                final Object o = session.get(DomsObject.class, object);
                if (o != null) {
                    session.save(o);
                } else {
                    session.save(object);
                }
            }
            //Alternative was to delete all and create those that was needed
            log.debug("Delete all other object from record {},{},{}", entryPid, viewAngle, date);
            session.createQuery("delete DomsObject d where d.entryPid= :pid and d.viewAngle= :viewAngle and d.objectPid not in :contained")
                   .setParameter("pid", entryPid)
                   .setParameter("viewAngle", viewAngle)
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
}
