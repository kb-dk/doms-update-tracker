package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.annotations.Cascade;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
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


    @ManyToMany(mappedBy = "objects")
    private Set<Record> records = new HashSet<Record>();

    public DomsObject() {
    }

    public DomsObject(String objectPid) {
        this.objectPid = objectPid;
    }


    public DomsObject(String objectPid, Set<Record> records) {
        this.objectPid = objectPid;
        this.records = records;
    }

    public String getObjectPid() {
        return objectPid;
    }

    public void setObjectPid(String objectPid) {
        this.objectPid = objectPid;
    }

    public Set<Record> getRecords() {
        return Collections.unmodifiableSet(new HashSet<Record>(records));
    }

    //TODO this name sucks
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

    @Override
    public int hashCode() {
        int result = objectPid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DomsObject{" +
               "objectPid='" + objectPid + '\'' +
               '}';
    }
}

