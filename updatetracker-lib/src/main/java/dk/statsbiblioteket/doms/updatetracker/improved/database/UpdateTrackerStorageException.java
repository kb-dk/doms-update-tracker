package dk.statsbiblioteket.doms.updatetracker.improved.database;

public class UpdateTrackerStorageException extends Exception{
    public UpdateTrackerStorageException(String message) {
        super(message);
    }

    public UpdateTrackerStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
