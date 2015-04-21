package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;

import java.util.List;

public class AbstractDB {

    private final SharedSessionContract session;

    public AbstractDB(SharedSessionContract session) {
        this.session = session;
    }

    @SuppressWarnings("unchecked")
    <T> List<T> listRecords(Query query) {
        return query.list();
    }

    @SuppressWarnings("unchecked")
    <T> List<T> listRecords(Criteria criteria) {
        return criteria.list();
    }


    public Transaction beginTransaction() {
        return session.beginTransaction();
    }
}
