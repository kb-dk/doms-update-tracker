package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

@NamedQueries({@NamedQuery(name = "ActiveAndDeleted",
                           query = "from Record e where (" +
                                       "(e.deleted is not null and e.deleted>=:since) or " +
                                       "(e.active is not null and e.active>=:since) " +
                                   ") and " +
                                   "e.viewAngle=:viewAngle and " +
                                   "e.collection=:collection " +
                                   "order by case " +
                                          "when e.deleted is not null and (e.active is null or e.deleted>=e.active) then e.deleted " +
                                          "when e.active is not null and (e.deleted is null or e.active>=e.deleted) then e.active " +
                                          " end"),

               @NamedQuery(name = "InactiveOrDeleted",
                           query = "from Record e where (" +
                                       "(e.deleted is not null and e.deleted>=:since) or " +
                                       "(e.inactive is not null or e.inactive>=:since)" +
                                   ") and " +
                                   "e.viewAngle=:viewAngle " +
                                   "and e.collection=:collection " +
                                   "order by case " +
                                          "when e.deleted is not null and (e.inactive is null or e.deleted>=e.inactive) then e.deleted " +
                                          "when e.inactive is not null and (e.deleted is null or e.inactive>=e.deleted) then e.inactive" +
                                          " end"),

               @NamedQuery(name = "Deleted",
                           query = "from Record e where (e.deleted is not null and e.deleted>=:since) and e.viewAngle=:viewAngle and e.collection=:collection order by e.deleted asc "),

               @NamedQuery(name = "All",
                           query = "from Record e where " +
                                   "(" +
                                       "(e.deleted is not null and e.deleted>=:since) or " +
                                       "(e.active is not null and e.active>=:since) or " +
                                       "(e.inactive is not null and e.inactive>=:since)" +
                                   ") and " +
                                   "e.viewAngle=:viewAngle and " +
                                   "e.collection=:collection " +
                                   "order by case " +
                                          "when e.deleted is not null and (e.active is null or e.deleted>=e.active) and (e.inactive is null or e.deleted>=e.inactive) then e.deleted " +
                                          "when e.active is not null and (e.deleted is null or e.active>=e.deleted) and (e.inactive is null or e.active>=e.inactive) then e.active " +
                                          "when e.inactive is not null and (e.deleted is null or e.inactive>=e.deleted) and ( e.active is null or e.inactive>=e.active) then e.inactive " +
                                          " end")}

)

/**
 * This is the ENTRIES table in the persistent storage. The ENTRIES table lists the entry objects/
 * Notice that since entryPid, viewAngle and state are naturalIds, the uniqneness key will be a combination. This
 * means that there will be multipe "rows" for each entryPid, corresponding to the different states and viewAngles.
 */
@Entity
@Table(name = "RECORDS")
public class Record implements Serializable {

    public enum State {
        ACTIVE("A"), INACTIVE("I"),DELETED("D");
        private final String name;

        State(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static State fromName(String name){
            for (State state : values()) {
                if (state.getName().equals(name)){
                    return state;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** The pid of the object */
    @Id
    @Column(name = "ENTRYPID")
    private String entryPid;

    /** The viewangle the object is an entry for */
    @Id
    @Column(name = "VIEWANGLE")
    private String viewAngle;

    @Id
    @Column(name = "COLLECTION")
    private String collection;

    private Timestamp active = null;

    private Timestamp inactive = null;

    private Timestamp deleted = null;

    @ManyToMany(cascade = {CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH})
    @JoinTable(name = "MEMBERSHIPS",
                      joinColumns = {@JoinColumn(referencedColumnName = "ENTRYPID"), @JoinColumn(referencedColumnName = "VIEWANGLE"),@JoinColumn(referencedColumnName = "COLLECTION")},
                      inverseJoinColumns = @JoinColumn(referencedColumnName = "OBJECTPID"))
    private Set<DomsObject> objects = new HashSet<>();

    public Record() {
    }

    public Record(String entryPid, String viewAngle, String collection) {
        this.entryPid = entryPid;
        this.viewAngle = viewAngle;
        this.collection = collection;
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

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
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
    public Timestamp getDateForChange(){
        switch (getState()){
            case ACTIVE:return active;
            case INACTIVE: return inactive;
            case DELETED: return deleted;
            default: return null;
        }
    }

    private Timestamp real(Timestamp timestamp) {
        if (timestamp == null){
            return new Timestamp(0);
        }
        return timestamp;
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

    public Timestamp getActive() {
        return active;
    }

    public void setActive(Timestamp active) {
        this.active = active;
    }

    public Timestamp getInactive() {
        return inactive;
    }

    public void setInactive(Timestamp inactive) {
        this.inactive = inactive;
    }

    public Timestamp getDeleted() {
        return deleted;
    }

    public void setDeleted(Timestamp deleted) {
        this.deleted = deleted;
    }

    public Set<DomsObject> getObjects() {
        return objects;
    }

    public void setObjects(Set<DomsObject> objects) {
        this.objects = objects;
    }
}
