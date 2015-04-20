package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Date;


public class LastModifiableInterceptor extends EmptyInterceptor {

    @Override
    public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState,
                                String[] propertyNames, Type[] types) {
        return updateLastModified(entity, currentState, propertyNames);
    }

    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        return updateLastModified(entity, state, propertyNames);
    }


    private boolean updateLastModified(Object entity, Object[] currentState, String[] propertyNames) {
        if (entity instanceof LastModifiable) {
            for (int i = 0; i < propertyNames.length; i++) {
                if ("LASTMODIFIED".equalsIgnoreCase(propertyNames[i])) {
                    currentState[i] = new Date();
                    return true;
                }
            }
        }
        return false;
    }

}
