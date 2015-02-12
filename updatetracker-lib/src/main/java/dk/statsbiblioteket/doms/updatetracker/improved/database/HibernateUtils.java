package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.Criteria;
import org.hibernate.Query;

import java.util.List;

public class HibernateUtils {
    @SuppressWarnings("unchecked")
    static <T> List<T> listAndCast(Criteria criteria) {
        return criteria.list();
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> listAndCast(Query query) {
        return query.list();
    }
}
