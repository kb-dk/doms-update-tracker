package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

public class RecordKey implements Serializable {

    /** The pid of the object */
    @Id
    @Column(name = "ENTRYPID", length = 64, nullable = false)
    private String entryPid;

    /** The viewangle the object is an entry for */
    @Id
    @Column(name = "VIEWANGLE", length = 64, nullable = false)
    private String viewAngle;

    @Id
    @Column(name = "COLLECTION", length = 64, nullable = false)
    private String collection;

    public RecordKey(String entryPid, String viewAngle, String collection) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.collection = collection;
    }

    public String getEntryPid() {
        return entryPid;
    }

    public String getViewAngle() {
        return viewAngle;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordKey)) {
            return false;
        }

        RecordKey recordKey = (RecordKey) o;

        if (!collection.equals(recordKey.collection)) {
            return false;
        }
        if (!entryPid.equals(recordKey.entryPid)) {
            return false;
        }
        if (!viewAngle.equals(recordKey.viewAngle)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = entryPid.hashCode();
        result = 31 * result + viewAngle.hashCode();
        result = 31 * result + collection.hashCode();
        return result;
    }
}
