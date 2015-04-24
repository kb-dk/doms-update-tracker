package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
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
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE, LASTMODIFIED " +
                                        "FROM RECORDS " +
                                        "WHERE " +
                                        "   LASTMODIFIED >= :since " +
                                        "   AND VIEWANGLE = :viewAngle " +
                                        "   AND COLLECTION = :collection " +
                                        "   AND ( " +
                                        "       ACTIVE is not NULL " +
                                        "       OR DELETED is not NULL " +
                                        "   )" +
                                        "ORDER BY LASTMODIFIED, ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "InactiveOrDeleted",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE, LASTMODIFIED " +
                                        "FROM RECORDS " +
                                        "WHERE " +
                                        "   LASTMODIFIED >= :since " +
                                        "   AND VIEWANGLE = :viewAngle " +
                                        "   AND COLLECTION = :collection " +
                                        "   AND ( " +
                                        "       INACTIVE is not NULL " +
                                        "       OR DELETED is not NULL " +
                                        "   )" +
                                        "ORDER BY LASTMODIFIED, ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "Deleted",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE, LASTMODIFIED " +
                                        "FROM RECORDS as r " +
                                        "WHERE " +
                                        "   LASTMODIFIED >= :since " +
                                        "   AND VIEWANGLE = :viewAngle " +
                                        "   AND COLLECTION = :collection " +
                                        "   AND DELETED is not NULL " +
                                        "ORDER BY LASTMODIFIED, ENTRYPID " +
                                        "LIMIT :limit "),
                            @NamedNativeQuery(
                                      resultClass = Record.class,
                                      name = "All",
                                      query =
                                        "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE, LASTMODIFIED " +
                                        "FROM RECORDS as r " +
                                        "WHERE " +
                                        "   LASTMODIFIED >= :since " +
                                        "   AND VIEWANGLE = :viewAngle " +
                                        "   AND COLLECTION = :collection " +
                                        "ORDER BY LASTMODIFIED, ENTRYPID " +
                                        "LIMIT :limit"),
                            @NamedNativeQuery(
                                      name = "UpdateDates",
                                      query =
                                        "UPDATE RECORDS as r " +
                                        "SET INACTIVE=:timestamp, " +
                                        "   ACTIVE=( " +
                                        "       CASE " +
                                        "           WHEN ACTIVE>=INACTIVE " +
                                        "           THEN :timestamp " +
                                        "           ELSE ACTIVE " +
                                        "       END " +
                                        "   ), " +
                                        "   LASTMODIFIED=:now " +
                                        "WHERE " +
                                        "   (r.ENTRYPID,r.VIEWANGLE,r.COLLECTION) in " +
                                        "       (" +
                                        "           SELECT m.ENTRYPID,m.VIEWANGLE,m.COLLECTION " +
                                        "           FROM MEMBERSHIPS as m " +
                                        "           WHERE m.OBJECTPID = :pid " +
                                        "       ) " +
                                        "   AND (r.DELETED is null or r.INACTIVE >= r.DELETED);"


                            ),
                            @NamedNativeQuery(
                                    name = "GetRecordsForPid",
                                    resultClass = Record.class,
                                    query =
                                      "SELECT VIEWANGLE, ENTRYPID, COLLECTION, ACTIVE, DELETED, INACTIVE, LASTMODIFIED " +
                                      "FROM RECORDS " +
                                      "   JOIN MEMBERSHIPS USING (VIEWANGLE,ENTRYPID,COLLECTION) " +
                                      "WHERE MEMBERSHIPS.OBJECTPID = :pid"
                            )

})


/**
 * This is the RECORDS table in the persistent storage. The RECORDS table lists the records that can be found in DOMS.
 */
@Entity
@Table(name = "RECORDS")
public class Record extends LastModifiable implements Serializable {

    public enum State {
        ACTIVE("A", "Published"),
        INACTIVE("I", "Inactive"),
        DELETED("D", "Deleted");
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

        public static State fromName(String name) {
            for (State state : values()) {
                if (state.getShortName().equals(name)) {
                    return state;
                }
                if (state.getLongName().equalsIgnoreCase(name)) {
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
    @Column(name = "ACTIVE", columnDefinition = "timestamp with time zone", nullable = true)
    private Date active = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "INACTIVE", columnDefinition = "timestamp with time zone", nullable = true)
    private Date inactive = null;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "DELETED", columnDefinition = "timestamp with time zone", nullable = true)
    private Date deleted = null;

    @ElementCollection
    @CollectionTable(name = "MEMBERSHIPS",
                            joinColumns = {
                                                  @JoinColumn(referencedColumnName = "VIEWANGLE", name = "VIEWANGLE"),
                                                  @JoinColumn(referencedColumnName = "ENTRYPID", name = "ENTRYPID"),
                                                  @JoinColumn(referencedColumnName = "COLLECTION", name = "COLLECTION")
                            })
    @Column(name = "OBJECTPID", length = 64)
    private Set<String> objects = new HashSet<>();

    public Record() {
    }

    public Record(String entryPid, String viewAngle, String collection) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.collection = collection;
    }

    public Record(String entryPid, String viewAngle, String collection, Date active, Date inactive, Date deleted,
                  Date lastModified) {
        super(lastModified);
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
    public Date getDateForChange() {
        switch (getState()) {
            case ACTIVE:
                return active;
            case INACTIVE:
                return inactive;
            case DELETED:
                return deleted;
            default:
                return null;
        }
    }

    private Date real(Date timestamp) {
        if (timestamp == null) {
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
