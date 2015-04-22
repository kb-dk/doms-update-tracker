package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.LatestKey;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBUtils.listRecords;
import static org.hibernate.criterion.Restrictions.and;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Restrictions.not;

public class DB implements Closeable{

    private final Session session;

    public DB(SessionFactory sessionFactory) {
        Session session = sessionFactory.getCurrentSession();
        session.setFlushMode(FlushMode.COMMIT);
        this.session=session;
    }

    public Transaction beginTransaction() {
        return session.beginTransaction();
    }

    public void saveOrUpdate(Record newRecord) {
        session.saveOrUpdate(newRecord);
    }

    public void updateDates(String pid, Date timestamp) {
        final Query query = session.getNamedQuery("UpdateDates");
        query.setParameter("pid", pid);
        query.setParameter("timestamp", timestamp);
        query.setParameter("now", new Date());
        query.executeUpdate();
    }


    public Record recordExists(Record newRecord) {
        final Record persistent = (Record) session.get(Record.class, newRecord);
        return persistent;
    }


    public List<Record> getAllRecordsWithThisEntryPid(String pid) {
        return listRecords(session.createCriteria(Record.class)
                                  .add(eq("entryPid", pid)));
    }


    public Collection<Record> getRecordsForPid(String pid) {
        return listRecords(session.getNamedQuery("GetRecordsForPid").setString("pid", pid));
    }


    public List<Record> getRecordsNotInTheseCollectionsAndViewAngles(String entryPid,
                                                                     Collection<String> entryViewAngles,
                                                                     Collection<String> collections) {
        final Criteria criteria = session.createCriteria(Record.class).add(eq("entryPid", entryPid));

        if (!collections.isEmpty() && !entryViewAngles.isEmpty()) {
            criteria.add(not(and(in("collection", collections), in("viewAngle", entryViewAngles))));
        } else if (!collections.isEmpty()) {
            criteria.add(not(and(in("collection", collections))));
        } else if (!entryViewAngles.isEmpty()) {
            criteria.add(not(and(in("viewAngle", entryViewAngles))));
        }
        return listRecords(criteria);
    }

    public void setLatestKey(long key) {
        session.saveOrUpdate(new LatestKey(key));

    }

    public void flush() {
        session.flush();
    }

    @Override
    public void close() {
        session.close();
    }
}
