package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@NamedNativeQueries({
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "ActiveAndDeleted",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                        "FROM ( " +
                                            "(" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE DELETED >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY DELETED " +
                                                "LIMIT :limit " +
                                            ") UNION ( " +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE ACTIVE >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY ACTIVE " +
                                                "LIMIT :limit " +
                                            ") " +
                                        ") AS r " +
                                        "ORDER BY " +
                                            "CASE " +
                                                "WHEN (r.DELETED IS NOT NULL) " +
                                                    "AND (r.ACTIVE IS NULL OR r.DELETED>=r.ACTIVE) " +
                                                "THEN r.DELETED " +
                                                "WHEN (r.ACTIVE IS NOT NULL) " +
                                                    "AND (r.DELETED IS NULL OR r.ACTIVE>=r.DELETED) " +
                                                "THEN r.ACTIVE " +
                                             "END, " +
                                            "r.ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "InactiveOrDeleted",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                        "FROM ( " +
                                            "(" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE DELETED >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY DELETED " +
                                                "LIMIT :limit " +
                                            ") UNION (" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE INACTIVE >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY INACTIVE " +
                                                "LIMIT :limit " +
                                            ") " +
                                        ") AS r " +
                                        "ORDER BY " +
                                            "CASE " +
                                                "WHEN (r.DELETED IS NOT NULL) " +
                                                    "AND (r.INACTIVE IS NULL OR r.DELETED>=r.INACTIVE) " +
                                                "THEN r.DELETED " +
                                                "WHEN (r.INACTIVE IS NOT NULL) " +
                                                    "AND (r.DELETED IS NULL OR r.INACTIVE>=r.DELETED) " +
                                                "THEN r.INACTIVE " +
                                             "END, " +
                                            "r.ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "Deleted",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                        "FROM ( " +
                                            "(" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE DELETED >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY DELETED " +
                                                "LIMIT :limit " +
                                            ") " +
                                        ") AS r " +
                                        "ORDER BY " +
                                            "CASE " +
                                                "WHEN (r.DELETED IS NOT NULL) " +
                                                "THEN r.DELETED " +
                                             "END, " +
                                            "r.ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "All",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                        "FROM ( " +
                                            "(" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE DELETED >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY DELETED " +
                                                "LIMIT :limit " +
                                            ") UNION ( " +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE ACTIVE >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY ACTIVE " +
                                                "LIMIT :limit " +
                                            ") UNION (" +
                                                "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE " +
                                                "FROM RECORDS " +
                                                "WHERE INACTIVE >= :since " +
                                                    "AND VIEWANGLE = :viewAngle " +
                                                    "AND COLLECTION = :collection " +
                                                "ORDER BY INACTIVE " +
                                                "LIMIT :limit " +
                                            ") " +
                                        ") AS r " +
                                        "ORDER BY " +
                                            "CASE " +
                                                "WHEN (r.DELETED IS NOT NULL) " +
                                                    "AND (r.ACTIVE IS NULL OR r.DELETED>=r.ACTIVE) " +
                                                    "AND (r.INACTIVE IS NULL OR r.DELETED>=r.INACTIVE) " +
                                                "THEN r.DELETED " +
                                                "WHEN (r.INACTIVE IS NOT NULL) " +
                                                    "AND (r.ACTIVE IS NULL OR r.INACTIVE>=r.ACTIVE) " +
                                                    "AND (r.DELETED IS NULL OR r.INACTIVE>=r.DELETED) " +
                                                "THEN r.INACTIVE " +
                                                "WHEN (r.ACTIVE IS NOT NULL) " +
                                                    "AND (r.INACTIVE IS NULL OR r.ACTIVE>=r.INACTIVE) " +
                                                    "AND (r.DELETED IS NULL OR r.ACTIVE>=r.DELETED) " +
                                                "THEN r.ACTIVE " +
                                             "END, " +
                                            "r.ENTRYPID " +
                                        "LIMIT :limit"),
                            @NamedNativeQuery(
                                      name = "updateDates",
                                      query =
                                        "UPDATE RECORDS as r " +
                                        "SET INACTIVE=:timestamp, " +
                                            "ACTIVE=( " +
                                                "CASE " +
                                                    "WHEN ACTIVE>=INACTIVE " +
                                                    "THEN :timestamp " +
                                                    "ELSE ACTIVE " +
                                                "END " +
                                            ") " +
                                        "WHERE " +
                                            "(r.ENTRYPID,r.VIEWANGLE,r.COLLECTION) in " +
                                                "(" +
                                                    "SELECT m.RECORD_ENTRYPID,m.RECORD_VIEWANGLE,m.RECORD_COLLECTION " +
                                                    "FROM MEMBERSHIPS as m " +
                                                    "WHERE m.OBJECTS = :pid " +
                                                ") " +
                                            "AND (r.DELETED is null or r.INACTIVE >= r.DELETED);"


                            ),
})


/**
 * This is the RECORDS table in the persistent storage. The RECORDS table lists the records that can be found in DOMS.
 */
@Entity
@Table(name = "RECORDS")
public class Record implements Serializable {

    public enum State {
        ACTIVE("A","Published"), INACTIVE("I","Inactive"),DELETED("D","Deleted");
        private final String shortName;
        private final String longName;

        State(String shortName, String longName) {
            this.shortName = shortName;
            this.longName = longName;
        }

        public String getShortName() {
            return shortName;
        }

        public String getLongName() {
            return longName;
        }

        public static State fromName(String name){
            for (State state : values()) {
                if (state.getShortName().equals(name)){
                    return state;
                }
                if (state.getLongName().equalsIgnoreCase(name)){
                    return state;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return longName;
        }
    }

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

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "active",columnDefinition = "timestamp with time zone", nullable = true)
    private Date active = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="inactive",columnDefinition = "timestamp with time zone", nullable = true)
    private Date inactive = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="deleted",columnDefinition = "timestamp with time zone", nullable = true)
    private Date deleted = null;

    @ElementCollection
    @CollectionTable(name = "memberships")
    @Column(name = "objects", length = 64)
    private Set<String> objects = new HashSet<String>();

    public Record() {
    }

    public Record(String entryPid, String viewAngle, String collection) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.collection = collection;
    }

    public Record(String viewAngle, String entryPid, String collection, Date active, Date deleted, Date inactive) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.collection = collection;
        this.active = active;
        this.inactive = inactive;
        this.deleted = deleted;
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

    public Set<String> getObjects() {
        return objects;
    }

    @Transient
    public State getState() {
        long n_inactive = real(inactive).getTime();
        long n_active = real(active).getTime();
        long n_deleted = real(deleted).getTime();
        if (n_inactive > n_active && n_inactive > n_deleted) {
            return State.INACTIVE;
        }
        if (n_active >= n_inactive && n_active > n_deleted) {
            return State.ACTIVE;
        }
        if (n_deleted >= n_active && n_deleted >= n_inactive) {
            return State.DELETED;
        }
        return State.INACTIVE;
    }

    @Transient
    public Date getDateForChange(){
        switch (getState()){
            case ACTIVE:return active;
            case INACTIVE: return inactive;
            case DELETED: return deleted;
            default: return null;
        }
    }

    private Date real(Date timestamp) {
        if (timestamp == null){
            return new Date(Long.MIN_VALUE);
        }
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Record)) {
            return false;
        }

        Record record = (Record) o;

        if (active != null ? !active.equals(record.active) : record.active != null) {
            return false;
        }
        if (!collection.equals(record.collection)) {
            return false;
        }
        if (deleted != null ? !deleted.equals(record.deleted) : record.deleted != null) {
            return false;
        }
        if (!entryPid.equals(record.entryPid)) {
            return false;
        }
        if (inactive != null ? !inactive.equals(record.inactive) : record.inactive != null) {
            return false;
        }
        if (objects != null ? !objects.equals(record.objects) : record.objects != null) {
            return false;
        }
        if (!viewAngle.equals(record.viewAngle)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = entryPid.hashCode();
        result = 31 * result + viewAngle.hashCode();
        result = 31 * result + collection.hashCode();
        result = 31 * result + (active != null ? active.hashCode() : 0);
        result = 31 * result + (inactive != null ? inactive.hashCode() : 0);
        result = 31 * result + (deleted != null ? deleted.hashCode() : 0);
        result = 31 * result + (objects != null ? objects.hashCode() : 0);
        return result;
    }

    public Date getActive() {
        return active;
    }

    public void setActive(Date active) {
        this.active = active;
    }

    public Date getInactive() {
        return inactive;
    }

    public void setInactive(Date inactive) {
        this.inactive = inactive;
    }

    public Date getDeleted() {
        return deleted;
    }

    public void setDeleted(Date deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return "Record{" +
               "entryPid='" + entryPid + '\'' +
               ", viewAngle='" + viewAngle + '\'' +
               ", collection='" + collection + '\'' +
               ", active=" + active +
               ", inactive=" + inactive +
               ", deleted=" + deleted +
               ", objects=" + objects +
               '}';
    }
}
