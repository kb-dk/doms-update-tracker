package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.annotations.Cascade;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This is the OBJECTS table. This table lists all the objects in DOMS.
 *
 */
@Entity

@Table(name = "OBJECTS")
public class DomsObject implements Serializable {

    /**
     * The pid of the Object
     */
    @Id
    @Column(name = "OBJECTPID",length = 64)
    private String objectPid;

    @Id
    private RecordKey recordKey;

    public DomsObject() {
    }

    public DomsObject(String objectPid) {
        this.objectPid = objectPid;
    }


    public String getObjectPid() {
        return objectPid;
    }


    public Set<Record> getRecords() {
        return Collections.unmodifiableSet(new HashSet<Record>(records));
    }

    protected Set<Record> getRecords_() {
        return records;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DomsObject)) {
            return false;
        }

        DomsObject that = (DomsObject) o;

        if (!objectPid.equals(that.objectPid)) {
            return false;
        }
        return true;
    }


}

