package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Set;

@Entity
@Table(name = "COLLECTIONS")
public class Collection implements Serializable{

    @Id
    @Column(name = "COLLECTION_ID")
    private String collectionID;

    @Id
    @Column(name = "ENTRYPID")
    private String entryPid;

    public Collection(String collectionID, String entryPid) {
        this.collectionID = collectionID;
        this.entryPid = entryPid;
    }

    public Collection() {
    }

    public String getCollectionID() {
        return collectionID;
    }

    public void setCollectionID(String collectionID) {
        this.collectionID = collectionID;
    }

    public String getEntryPid() {
        return entryPid;
    }

    public void setEntryPid(String entryPid) {
        this.entryPid = entryPid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Collection)) {
            return false;
        }

        Collection that = (Collection) o;

        if (!collectionID.equals(that.collectionID)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return collectionID.hashCode();
    }
}
