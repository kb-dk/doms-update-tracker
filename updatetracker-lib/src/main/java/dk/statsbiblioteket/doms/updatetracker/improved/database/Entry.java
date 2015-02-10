package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.annotations.ForeignKey;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NotFound;
import org.hibernate.annotations.NotFoundAction;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is the ENTRIES table in the persistent storage. The ENTRIES table lists the entry objects/
 * Notice that since entryPid, viewAngle and state are naturalIds, the uniqneness key will be a combination. This
 * means that there will be multipe "rows" for each entryPid, corresponding to the different states and viewAngles.
 */
@Entity
@Table(name = "ENTRIES")
public class Entry implements Serializable {

    /** The pid of the object */
    @Id
    @Column(name = "ENTRYPID")
    private String entryPid;

    /** The viewangle the object is an entry for */
    @Id
    @Column(name = "VIEWANGLE")
    private String viewAngle;

    /** The state of the object */
    @Id
    @Column(name = "STATE")
    private String state;

    /** The date the object got to this configuration */
    private Timestamp dateForChange;

    public Entry() {
    }

    public Entry(String entryPid, String viewAngle, String state, Date dateForChange) {
        this(entryPid, viewAngle, state, new Timestamp(dateForChange.getTime()));
    }


    public Entry(String entryPid, String viewAngle, String state, Timestamp dateForChange) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.state = state;
        this.dateForChange = dateForChange;
    }


    public String getEntryPid() {
        return entryPid;
    }

    public void setEntryPid(String entryPid) {
        this.entryPid = entryPid;
    }

    public String getViewAngle() {
        return viewAngle;
    }

    public void setViewAngle(String viewAngle) {
        this.viewAngle = viewAngle;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Timestamp getDateForChange() {
        return dateForChange;
    }

    public void setDateForChange(Timestamp dateForChange) {
        this.dateForChange = dateForChange;
    }

    public void setDateForChange(Date dateForChange) {
        setDateForChange(new Timestamp(dateForChange.getTime()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Entry)) {
            return false;
        }

        Entry entry = (Entry) o;

        if (dateForChange != null ? !dateForChange.equals(entry.dateForChange) : entry.dateForChange != null) {
            return false;
        }
        if (entryPid != null ? !entryPid.equals(entry.entryPid) : entry.entryPid != null) {
            return false;
        }
        if (state != null ? !state.equals(entry.state) : entry.state != null) {
            return false;
        }
        if (viewAngle != null ? !viewAngle.equals(entry.viewAngle) : entry.viewAngle != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = entryPid != null ? entryPid.hashCode() : 0;
        result = 31 * result + (viewAngle != null ? viewAngle.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (dateForChange != null ? dateForChange.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "Entry{" +
               ", entryPid='" + entryPid + '\'' +
               ", viewAngle='" + viewAngle + '\'' +
               ", state='" + state + '\'' +
               ", dateForChange=" + dateForChange +
               '}';
    }


}
