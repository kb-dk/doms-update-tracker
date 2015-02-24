package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.LogicalExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.HibernateUtils.listAndCast;
import static java.util.stream.Collectors.toSet;
import static org.hibernate.criterion.Restrictions.and;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Restrictions.not;

public class UpdateTrackerBackend {
    private Fedora fedora;
    private Logger log = LoggerFactory.getLogger(UpdateTrackerBackend.class);


    public UpdateTrackerBackend(Fedora fedora) {
        this.fedora = fedora;
    }

    /**
     * Modify the persistent storage regarding a change.
     *  @param pid     the pid of the object that was changed
     * @param date    the date of the change
     * @param collection
     * @param state   the state of the entries that should be updated
     * @param session
     */
    public void modifyState(String pid, Timestamp date, String collection, State state, Session session) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {
        log.debug("Starting modifyState({},{},{})", pid, date, state);
        /*
        if new state is not Deleted
            If the object was not previously known in RECORDS
                if the object is an Entry object # Check content models vs. content model cache
                    Create row in RECORDS and OBJECTS denoting this Record, with the Inactive Timestamp set
                    if the new state is Active
                        also set the Active Timestamp
            else
                For each row in RECORDS with entryPid = this pid
                    set Inactive Timestamp
                    if new state is Active
                        set Active Timestamp
        If the new state is Deleted
            Get all Records containing this pid from OBJECTS and not this pid as entryPid #This gets all the other records that could be affected by this delete
            For each of these Records
                reconnectObjects(record.entryPid) # Recalculate the records
            Delete all rows with objectPid=this pid from OBJECTS # Remove reference to this object
            For each Record with entryPid = this pid # And mark is as deleted if it is an entry
                set Deleted Timestamp
                unset Active and Inactive Timestamp


         */
        if ( state != State.DELETED) {
            List<Record> thisRecords = listAndCast(session.createCriteria(Record.class).add(eq("entryPid", pid)));
            if (thisRecords.isEmpty()){
                List<ViewInfo> viewInfo = fedora.getViewInfo(pid, date);
                List<String> entryForViewAngles = isEntryForViewAngles(viewInfo);
                for (String entryForViewAngle : entryForViewAngles) {
                    log.debug("Pid {} is an entry for viewangle {}", pid, entryForViewAngle);
                    Record record = new Record(pid, entryForViewAngle, collection);
                    Object actual = session.get(Record.class, record);
                    if (actual == null) {
                        log.debug("Pid {} is not marked as an entry for viewAngle {}. Fixing", pid, entryForViewAngle);
                        final DomsObject object = get(session,new DomsObject(pid));
                        record.getObjects().add(object);
                        record.setInactive(date);
                        if (state == State.ACTIVE){
                            record.setActive(date);
                        }
                        session.saveOrUpdate(record);
                    }
                }
            } else {
                for (Record record : thisRecords) {
                    record.setInactive(date);
                    if (state == State.ACTIVE) {
                        record.setActive(date);
                    }
                    session.saveOrUpdate(record);
                }
            }
        }
        if (state == State.DELETED){
            log.debug("Switching on states for pid {}, got the Deleted branch", pid);

            //TODO This code duplicates code in recalc view
            List<DomsObject> thisObjects = listAndCast(session.createCriteria(DomsObject.class).add(eq("objectPid", pid)));
            Set<Record> otherRecords = thisObjects.stream()
                                                    .map(DomsObject::getRecords)
                                                    .flatMap(Collection::stream)
                                                    .filter(record -> !record.getEntryPid().equals(pid))
                                                    .collect(toSet());
            for (Record otherRecord : otherRecords) {
                ViewBundle bundle = fedora.calcViewBundle(otherRecord.getEntryPid(), otherRecord.getViewAngle(), date);
                Set<DomsObject> before = new HashSet<>(otherRecord.getObjects());
                otherRecord.getObjects().clear();
                for (String viewObject : bundle.getContained()) {
                    log.debug("Marking object {} as part of record {},{},{}", viewObject, otherRecord.getEntryPid(), otherRecord.getViewAngle(), otherRecord.getCollection());
                    final DomsObject object = new DomsObject(pid);
                    otherRecord.getObjects().add(object);
                }
                if (before.equals(otherRecord.getObjects())){
                    if (otherRecord.getInactive().equals(otherRecord.getActive())){
                        otherRecord.setActive(date);
                    }
                    otherRecord.setInactive(date);
                    session.saveOrUpdate(otherRecord);
                }
                before.removeAll(otherRecord.getObjects());
                for (DomsObject domsObject : before) {
                    session.delete(domsObject);
                }
            }
            session.createQuery("delete DomsObject d where d.objectPid= :pid").setParameter("pid", pid).executeUpdate();

            List<Record> thisRecords = listAndCast(session.createCriteria(Record.class).add(eq("entryPid", pid)));
            thisRecords.stream().forEach(record -> {
                record.setDeleted(date);
                record.setInactive(null);
                record.setActive(null);
                session.saveOrUpdate(record);
            });
        }

    }

    List<String> isEntryForViewAngles(List<ViewInfo> viewInfo) {
        return viewInfo.stream().filter(ViewInfo::isEntry).map(ViewInfo::getViewAngle).collect(Collectors.toList());
    }


    public void modifyRelations(String pid, Timestamp date, Session session) throws
                                                                 FedoraFailedException,
                                                                 UpdateTrackerStorageException {
        /*
        Get the view Information about this object (Which viewAngles is this object entry for)
        get the Collection information about this object (which collections is it in)
        Create Records in RECORDS corresponding to all these view angles and collections (if they do not exist already) with the Inactive Timestamp set
        For each Record in RECORDS with this entry pid and not in this set of view angles or not in this set of collections
            Set Deleted Timestamp
            unset Inactive and Active Timestamp
            remove all objects from OBJECTS linked to this Record
        for each Record this object is part of (query OBJECTS with objectPid = this pull)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS
         */

        log.debug("starting modifyRelations({},{})",pid,date);

        //Create new Records
        final Collection<String> entryForViewAngles = isEntryForViewAngles(fedora.getViewInfo(pid, date));
        final Collection<String> collections = fedora.getCollections(pid, asDate(date));
        for (String entryForViewAngle : entryForViewAngles) {
            for (String collection : collections) {
                Record record = new Record(pid, entryForViewAngle, collection);
                if (session.get(Record.class,record) == null){
                    DomsObject object = get(session, new DomsObject(pid));
                    record.getObjects().add(object);
                    record.setInactive(date);
                    session.saveOrUpdate(record);
                }
            }
        }
        //Remove old records
        //"not (A and B)" is the same as "(not A) or (not B)"
        final Criteria criteria = session.createCriteria(Record.class).add(eq("entryPid", pid));

        if (!collections.isEmpty() && !entryForViewAngles.isEmpty()){
            criteria.add(not(and(in("collection", collections), in("viewAngle", entryForViewAngles))));
        } else if (!collections.isEmpty()){
            criteria.add(not(and(in("collection", collections))));
        } else if (!entryForViewAngles.isEmpty()){
            criteria.add(not(and(in("viewAngle", entryForViewAngles))));
        }
        List<Record> previousRecords = listAndCast(criteria);

        for (Record previousRecord : previousRecords) {
            previousRecord.setDeleted(date);
            previousRecord.setInactive(null);
            previousRecord.setActive(null);
            previousRecord.getObjects().stream().forEach(session::delete);
            previousRecord.getObjects().clear();
            session.saveOrUpdate(previousRecord);
        }

        //Update other records


        recalculateView(pid, date, session);
    }

    private DomsObject get(Session session, DomsObject domsObject) {
        Object persistent = session.get(DomsObject.class, domsObject.getObjectPid());
        if (persistent != null){
            return (DomsObject) persistent;
        } else {
            return domsObject;
        }
    }

    private Date asDate(Timestamp date) {
        return new Date(date.getTime());
    }

    private void recalculateView(String pid, Timestamp date, Session session) throws FedoraFailedException {
        log.debug("Recalculating view for {}",pid);
        /*
        for each Record this object is part of (query OBJECTS with objectPid = this pull)
            remove all Objects relating to this Record from OBJECTS
            recalculate view of entryPid/viewAngle
            update OBJECTS

         */

        List<Record> records = listAndCast(session.createQuery("select d.records from DomsObject d where d.objectPid= :pid")
                                                      .setParameter("pid", pid));
        log.debug("Find all records {} containing {} ",records,pid);
        for (Record otherRecord : records) {
            ViewBundle bundle = fedora.calcViewBundle(otherRecord.getEntryPid(), otherRecord.getViewAngle(), date);
            Set<DomsObject> before = new HashSet<>(otherRecord.getObjects());
            otherRecord.getObjects().clear();
            for (String viewObject : bundle.getContained()) {
                log.debug("Marking object {} as part of record {},{},{}",
                                 viewObject,
                                 otherRecord.getEntryPid(),
                                 otherRecord.getViewAngle(),
                                 otherRecord.getCollection());
                DomsObject object = (DomsObject) session.get(DomsObject.class, viewObject);
                if (object == null){
                    object = new DomsObject(viewObject);
                }
                otherRecord.getObjects().add(object);
            }
            if (before.equals(otherRecord.getObjects())) {
                if (otherRecord.getInactive().equals(otherRecord.getActive())) {
                    otherRecord.setActive(date);
                }
                otherRecord.setInactive(date);
            }
            session.saveOrUpdate(otherRecord);
        }
    }


    public void updateTimestamps(String pid, Timestamp date, Session session) {

            final Query query
                    = session.createQuery("update Record e set e.inactive=:date, e.active=(case when e.active>=e.inactive then :date else e.active end) where :pid member of e.objects and e.deleted is null or e.inactive>=e.deleted");
            query.setParameter("pid", pid);
            query.setParameter("date", date);
            query.executeUpdate();


        //,
    }


    public boolean isContentModel(String pid, Session session) {
        return false;
    }
}
