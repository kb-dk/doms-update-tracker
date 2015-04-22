package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.LatestKey;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;

import java.io.Closeable;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBUtils.listRecords;

public class StatelessDB implements Closeable{

    private final StatelessSession session;

    public StatelessDB(SessionFactory sessionFactory) {
        this.session = sessionFactory.openStatelessSession();
    }

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
             .setLong("limit", limit);

        return listRecords(query);
    }


    @Override
    public void close() {
        session.close();
    }

    public long getLatestKey() {
        List<LatestKey> list = session.createCriteria(LatestKey.class).list();
        if (list.size() > 0) {
            return list.get(0).getKey();
        } else {
            return 0L;
        }
    }

    public Date getLastChangedTimestamp() {
        final Query query = session.createQuery("select max(e.inactive) from Record e");
        query.setMaxResults(1);
        Object result = query.uniqueResult();
        if (result != null) {
            if (result instanceof Date) {
                return (Date) result;
            }
        }
        return null;
    }
}
