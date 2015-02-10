package dk.statsbiblioteket.doms.updatetracker.improved.database;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "CONTENTMODELS")
public class ContentModel implements Serializable{

    @Id
    private String cmPid;

    @Id
    @Column(name = "VIEWANGLE")
    private String viewAngle;

    @Column(name = "ENTRYCONTENTMODEL")
    private boolean entryContentModel;

    public ContentModel(String cmPid, String viewAngle, boolean isEntry) {
        this.cmPid = cmPid;
        this.viewAngle = viewAngle;
        this.entryContentModel = isEntry;
    }

    public ContentModel() {
    }

    public String getCmPid() {
        return cmPid;
    }

    public void setCmPid(String pid) {
        this.cmPid = pid;
    }

    public String getViewAngle() {
        return viewAngle;
    }

    public void setViewAngle(String viewAngle) {
        this.viewAngle = viewAngle;
    }

    public boolean isEntryContentModel() {
        return entryContentModel;
    }

    public void setEntryContentModel(boolean entry) {
        this.entryContentModel = entry;
    }
}
