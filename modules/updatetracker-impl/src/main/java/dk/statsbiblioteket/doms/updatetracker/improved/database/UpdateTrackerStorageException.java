package dk.statsbiblioteket.doms.updatetracker.improved.database;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/7/13
 * Time: 1:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackerStorageException extends Exception{
    public UpdateTrackerStorageException(String message) {
        super(message);
    }

    public UpdateTrackerStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
