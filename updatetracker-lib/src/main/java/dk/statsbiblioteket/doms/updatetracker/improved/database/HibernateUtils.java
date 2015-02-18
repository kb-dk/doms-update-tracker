package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.Criteria;
import org.hibernate.Query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HibernateUtils {
    @SuppressWarnings("unchecked")
    static <T> List<T> listAndCast(Criteria criteria) {
        return criteria.list();
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> listAndCast(Query query) {
        return query.list();
    }

    static <T> Set<T> set(T... vars){
        return new HashSet<>(Arrays.asList(vars));
    }
}
