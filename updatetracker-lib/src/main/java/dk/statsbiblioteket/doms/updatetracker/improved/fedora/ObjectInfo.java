package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/7/13
 * Time: 2:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class ObjectInfo {

    private String objectPid;

    private Date lastModified;

    private String state;

    private List<String> entryForViewangles;


    public ObjectInfo() {
        entryForViewangles = new LinkedList<String>();
    }

    public String getObjectPid() {
        return objectPid;
    }

    public void setObjectPid(String objectPid) {
        this.objectPid = objectPid;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public boolean add(String s) {
        return entryForViewangles.add(s);
    }
}
