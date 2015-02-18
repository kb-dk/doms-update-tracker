package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;

import java.util.Date;
import java.util.List;

/**
 * This is the interface representing the storage system, which holds the updates from doms.
 * Before using it, be sure to call setup()
 * The methods represents events that can take place, except the lookup method
 */
public interface UpdateTrackerPersistentStore extends AutoCloseable{

    void setUp() throws Exception;

    void clear();

    void dumpToStdOut();

    /**
     * Invoke to register a new object, that has been created
     *
     * @param pid
     * @param date
     */
    void objectCreated(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException;

    /**
     * Object was changed to the deleted state. Mark any "Deleted" entries to reflect this
     *
     * @param pid  the pid of the object
     * @param date the date of the change
     */
    void objectDeleted(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException;


    /**
     * Object was changed, but remains in the inProgress state
     *
     * @param pid
     * @param date
     * @param dsid
     */
    void datastreamChanged(String pid, Date date, String dsid) throws
                                                               UpdateTrackerStorageException,
                                                               FedoraFailedException;


    /**
     * Objects have changes in the relations, so we will have to update the structure of the views
     *
     * @param pid
     * @param date
     */
    void objectRelationsChanged(String pid, Date date) throws UpdateTrackerStorageException, FedoraFailedException;

    /**
     * The object have (potentially) changed state
     * @param pid
     * @param date
     * @param newstate
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    void objectStateChanged(String pid, Date date, String newstate) throws
                                                                    UpdateTrackerStorageException,
                                                                    FedoraFailedException;


    /**
     * Find objects from the database. TODO collections viewangles restrict
     *
     * @param since
     *
     * @return
     */
    public List<Record> lookup(Date since, String viewAngle, int offset, int limit, String state, String collection) throws UpdateTrackerStorageException;

        @Override
    void close();
}
