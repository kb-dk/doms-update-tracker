package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import org.hibernate.Criteria;
import org.hibernate.Query;

import java.util.List;

public class DBUtils {
    @SuppressWarnings("unchecked")
    static <T> List<T> listRecords(Query query) {
        return query.list();
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> listRecords(Criteria criteria) {
        return criteria.list();
    }



}
