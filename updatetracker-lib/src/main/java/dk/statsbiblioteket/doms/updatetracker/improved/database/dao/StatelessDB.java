package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record;
import org.hibernate.Query;
import org.hibernate.StatelessSession;

import java.util.Date;
import java.util.List;

public class StatelessDB extends AbstractDB {

    private final StatelessSession session;

    public StatelessDB(StatelessSession session) {
        super(session);
        this.session = session;
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


}
