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

import static org.hibernate.criterion.Restrictions.and;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Restrictions.not;

/**
 * This class represent the database access layer of the update tracker. All the persistence operations needed should exist here
 * This provide a clear abstraction of the database and allows for easy database mocking
 */
public class DB implements Closeable{

    private final Session session;
    private final boolean readonly;

    /**
     * create a new database connection
     * @param sessionFactory Will create a new session from the session factory with the correct modes and props set
     */
    DB(SessionFactory sessionFactory, boolean readonly) {
        this.readonly = readonly;
        Session session = sessionFactory.getCurrentSession();
        session.setFlushMode(FlushMode.COMMIT);
        this.session=session;
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> listRecords(Query query) {
        return query.list();
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> listRecords(Criteria criteria) {
        return criteria.list();
    }

    /**
     * Begin a new database transaction
     * @return the transaction object
     */
    public Transaction beginTransaction() {
        final Transaction transaction = session.beginTransaction();
        session.setDefaultReadOnly(readonly);
        return transaction;
    }

    /**
     * Save this record with the changes in the database
     * @param newRecord the record to save
     */
    public void saveRecord(Record newRecord) {
        session.saveOrUpdate(newRecord);
    }

    public void updateDates(String pid, Date timestamp) {
        final Query query = session.getNamedQuery("UpdateDates");
        query.setParameter("pid", pid);
        query.setParameter("timestamp", timestamp);
        query.executeUpdate();
    }


    /**
     * Get the record as it exists in the database, with the changes added in this session. Returns null if the
     * record do not exist in the database
     * @param newRecord the record to use as identifier
     * @return the persistent record with local changes or null
     */
    public Record getPersistentRecord(Record newRecord) {
        return (Record) session.get(Record.class, newRecord);
    }


    /**
     * Get all records with the given pid as entry pid
     * @param entryPid the entry pid
     * @return a list of records, possibly empty
     */
    public Collection<Record> getAllRecordsWithThisEntryPid(String entryPid) {
        return listRecords(session.createCriteria(Record.class)
                                  .add(eq("entryPid", entryPid)));
    }

    /**
     * Get all the records containing this pid in their objects
     * @param pid the pid
     * @return a list of records, possibly empty
     */
    public Collection<Record> getRecordsContainingThisPid(String pid) {
        return listRecords(session.getNamedQuery("GetRecordsForPid").setString("pid", pid));
    }

    /**
     * Get all the records which have this entrypid, but which are not part of the given collections and viewangles
     * @param entryPid the entry pid that the results must have
     * @param entryViewAngles all view angles that the results must NOT have
     * @param collections all collections that the results must NOT have
     * @return all the records for this entrypid and other collections and viewangles
     */
    public Collection<Record> getRecordsNotInTheseCollectionsAndViewAngles(String entryPid,
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

    /**
     * Update the lastmodified key
     * @see dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.LatestKey
     * @param key the new value of the key
     */
    public void setLatestKey(long key) {
        session.saveOrUpdate(new LatestKey(key));

    }

    /**
     * Get the records matching the given criteria
     * @param since modified since this timestamp
     * @param viewAngle from this viewangle
     * @param offset offset in the result set
     * @param limit max length of the returned list
     * @param state records in this state. Can be one of Record.State or null. No matter what you set this to, you will
     *              also get the deleted records
     * @param collection from this collection
     * @return a list of records, sorted by lastModified, matching these criteria.
     */
    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) {
        Query query;
        if (state == null) {
            query = session.getNamedQuery("All");
        } else {
            final Record.State fromName = Record.State.fromName(state);
            if (fromName == null) {
                query = session.getNamedQuery("All");
            } else {
                switch (fromName) {
                    case ACTIVE:
                        query = session.getNamedQuery("ActiveAndDeleted");
                        break;
                    case INACTIVE:
                        query = session.getNamedQuery("InactiveOrDeleted");
                        break;
                    case DELETED:
                        query = session.getNamedQuery("Deleted");
                        break;
                    default:
                        query = session.getNamedQuery("All");
                        break;
                }
            }
        }


        query.setReadOnly(true);
        query.setFirstResult(offset);
        query.setTimestamp("since", since)
             .setString("collection", collection)
             .setString("viewAngle", viewAngle)
             .setLong("maxResults", limit);

        return listRecords(query);
    }

    /**
     * Get the latestkey
     * @return the current latest key in the database
     */
    public long getLatestKey() {
        List<LatestKey> list = session.createCriteria(LatestKey.class).list();
        if (list.size() > 0) {
            return list.get(0).getKey();
        } else {
            return 0L;
        }
    }

    /**
     * Get the highest modification time in the database for a given viewangle and collection.
     * @return the highest last modified timestamp in the database
     * @param viewangle The viewangle to get modification time for
     * @param collection The collection to get modification time for
     */
    public Date getLastChangedTimestamp(String viewangle, String collection) {
        final Query query = session.createQuery("select max(e.lastModified) from Record e where collection=:collection and viewangle=:viewangle");
        query.setString("viewangle", viewangle);
        query.setString("collection", collection);
        query.setMaxResults(1);
        Object result = query.uniqueResult();
        if (result != null) {
            if (result instanceof Date) {
                return (Date) result;
            }
        }
        return null;
    }


    /**
     * Flush the database connection
     */
    public void flush() {
        session.flush();
    }

    /**
     * Close the database connection
     */
    @Override
    public void close() {
        session.close();
    }
}
