package dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;

/**
 * Utility class that gives this entity a lastModifiable field with getters and setters
 */
@MappedSuperclass()
public abstract class LastModifiable {

    public LastModifiable() {
        lastModified = new Date();
    }

    public LastModifiable(Date lastModified) {
        this.lastModified = lastModified;
    }

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "LASTMODIFIED", columnDefinition = "timestamp with time zone", nullable = true)
    protected Date lastModified;

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }
}
