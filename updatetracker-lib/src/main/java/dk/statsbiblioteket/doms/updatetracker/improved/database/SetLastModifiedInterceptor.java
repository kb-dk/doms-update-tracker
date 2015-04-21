package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Date;


/**
 * This interceptor should hook the save/flush methods. If you attempt to save or flush an entity of the
 * LastModifiable class, the lastModified field will be updated.
 * This interceptor must be registered in hibernate like this
 *<p>
 *         configuration.setInterceptor(new SetLastModifiedInterceptor());
 </p>
 *
 * @see dk.statsbiblioteket.doms.updatetracker.improved.database.LastModifiable
 */
public class SetLastModifiedInterceptor extends EmptyInterceptor {

    /**
     * Called during a flush, if an object is determined to be dirty. Dirty means that is have changed
     * compared to the database version
     * @param entity the dirty entity
     * @param id it's id
     * @param currentState the current state as a list of objects
     * @param previousState the previous state, ignore
     * @param propertyNames the names of the fields, use this to adress currentstate
     * @param types ignore
     * @return true if you changed the currentstate
     */
    @Override
    public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState,
                                String[] propertyNames, Type[] types) {
        return updateLastModified(entity, currentState, propertyNames);
    }


    /**
     * Called before an entity is created in the database (SQL INSERT). Allows us to set the lastModifiedTimestamp
     * @param entity the new entity to create
     * @param id the id of the entity
     * @param state the state of the new entity
     * @param propertyNames the names of the fields, use this to adress currentstate
     * @param types ignore
     * @return true if you modified the state
     */
    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        return updateLastModified(entity, state, propertyNames);
    }


    /**
     * Utility method to update the lastModified timestamp
     * @param entity the entity to update
     * @param state the value of the fields
     * @param propertyNames the names of the fields
     * @return true if the lastmodified was updated
     */
    private boolean updateLastModified(Object entity, Object[] state, String[] propertyNames) {
        if (entity instanceof LastModifiable) {
            for (int i = 0; i < propertyNames.length; i++) {
                if ("LASTMODIFIED".equalsIgnoreCase(propertyNames[i])) {
                    state[i] = new Date();
                    return true;
                }
            }
        }
        return false;
    }

}
