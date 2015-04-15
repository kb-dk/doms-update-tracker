package dk.statsbiblioteket.doms.updatetracker.improved.database;

import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import java.io.Serializable;
import java.util.Collections;
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
                                                    "SELECT m.RECORDS_ENTRYPID,m.RECORDS_VIEWANGLE,m.RECORDS_COLLECTION " +
                                                    "FROM MEMBERSHIPS as m " +
                                                    "WHERE m.OBJECTS_OBJECTPID = :pid " +
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

    @Id
    private RecordKey recordKey;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "active",columnDefinition = "timestamp with time zone", nullable = true)
    private Date active = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="inactive",columnDefinition = "timestamp with time zone", nullable = true)
    private Date inactive = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="deleted",columnDefinition = "timestamp with time zone", nullable = true)
    private Date deleted = null;

    @OneToMany
    @Cascade({CascadeType.REFRESH,CascadeType.REPLICATE,CascadeType.SAVE_UPDATE,CascadeType.PERSIST,CascadeType.MERGE})
    private Set<DomsObject> objects = new HashSet<DomsObject>();

    public Record() {
    }

    public void addObject(DomsObject object) {
        objects.add(object);
        object.getRecords_().add(this);
    }

    public void clearObjects() {
        for (DomsObject object : objects) {
            object.getRecords_().remove(this);
        }
        objects.clear();
    }


    public Set<DomsObject> getObjects() {
        return Collections.unmodifiableSet(objects);
    }


    public Record(String entryPid, String viewAngle, String collection) {
        this.recordKey = new RecordKey(entryPid,viewAngle,collection);
    }

    public Record(String viewAngle, String entryPid, String collection, Date active, Date deleted, Date inactive) {
        this.recordKey = new RecordKey(entryPid, viewAngle, collection);
        this.active = active;
        this.inactive = inactive;
        this.deleted = deleted;
    }

    public String getEntryPid() {
        return recordKey.getEntryPid();
    }

    public String getViewAngle() {
        return recordKey.getViewAngle();
    }

    public String getCollection() {
        return recordKey.getCollection();
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

        if (!recordKey.equals(record.recordKey)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return recordKey.hashCode();
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

}
