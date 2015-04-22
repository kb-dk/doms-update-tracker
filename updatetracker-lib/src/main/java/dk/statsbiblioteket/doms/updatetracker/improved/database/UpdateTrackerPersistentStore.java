package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;

import java.io.Closeable;
import java.util.Date;
import java.util.List;

/**
 * This is the interface representing the storage system, which holds the updates from doms.
 * The methods represents events that can take place, except the lookup method
 */
public interface UpdateTrackerPersistentStore extends Closeable {

    /**
     * Invoke to register a new object, that has been created
     *
     *  @param pid
     * @param date
     * @param key the key from the work log table, that defined this operation. This key will be persisted in the
     *            database as the latest work log key handled.
     */
    void objectCreated(String pid, Date date, long key) throws UpdateTrackerStorageException, FedoraFailedException;

    /**
     * Object was changed to the deleted state. Mark any "Deleted" entries to reflect this
     *
     *  @param pid  the pid of the object
     * @param date the date of the change
     * @param key the key from the work log table, that defined this operation. This key will be persisted in the
     *            database as the latest work log key handled.
     */
    void objectDeleted(String pid, Date date, long key) throws UpdateTrackerStorageException, FedoraFailedException;


    /**
     * Object was changed, but remains in the inProgress state
     *
     *  @param pid
     * @param date
     * @param dsid
     * @param key the key from the work log table, that defined this operation. This key will be persisted in the
     *            database as the latest work log key handled.
     */
    void datastreamChanged(String pid, Date date, String dsid, long key) throws
                                                               UpdateTrackerStorageException,
                                                               FedoraFailedException;


    /**
     * Objects have changes in the relations, so we will have to update the structure of the views
     *
     *  @param pid
     * @param date
     * @param key the key from the work log table, that defined this operation. This key will be persisted in the
     *            database as the latest work log key handled.
     */
    void objectRelationsChanged(String pid, Date date, long key) throws UpdateTrackerStorageException, FedoraFailedException;

    /**
     * The object have (potentially) changed state
     * @param pid
     * @param date
     * @param newstate
     * @param key the key from the work log table, that defined this operation. This key will be persisted in the
     *            database as the latest work log key handled.
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    void objectStateChanged(String pid, Date date, String newstate, long key) throws
                                                                    UpdateTrackerStorageException,
                                                                    FedoraFailedException;


    /**
     * Find objects from the database.
     *
     * @param since only return records changed since this date
     * @param viewAngle the viewangle to return objects for
     * @param offset offset into the list. Almost always 0
     * @param limit the max length of the result list
     * @param state The state of the results. Can be "A", which returns Active and Deleted. Can be "I", which returns
     *              Inactive and Deleted. Can be "D" which returns Deleted. Can be null, which returns Active, Inactive
     *              and Deleted
     * @param collection The collection to return objects for
     *
     * @return a list of records, sorted by the highest timestamp for the relevant states.
     */
    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) throws UpdateTrackerStorageException;


    /**
     * Get the timestamp of the last fedora operation that caused ANY change to the update tracker state
     * @return the last timestamp of any change in the update tracker
     * @throws UpdateTrackerStorageException
     */
    public Date lastChanged() throws UpdateTrackerStorageException;

        @Override
    void close();

    /**
     * Get the latest key processed from the work log table.
     * @return The latest key processed from the work log table.
     */
    long getLatestKey();
}
