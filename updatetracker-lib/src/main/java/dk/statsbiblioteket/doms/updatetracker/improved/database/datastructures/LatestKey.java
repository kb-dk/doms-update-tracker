package dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Stores the latest key from the worklog in the updatetracker database.
 */
@Entity
@Table(name = "latestKey")
public class LatestKey {
    @Id
    /** Fake identifier to fool hibernate to always update the same value */
    private boolean id = true;
    /** The latest key */
    private long key;

    public LatestKey() {
        setId(true);
    }

    public LatestKey(long key) {
        this();
        setKey(key);
    }

    public boolean isId() {
        return id;
    }

    public void setId(boolean id) {
        this.id = true;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }
}
