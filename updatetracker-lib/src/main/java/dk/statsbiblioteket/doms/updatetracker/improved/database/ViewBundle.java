package dk.statsbiblioteket.doms.updatetracker.improved.database;

import java.util.ArrayList;
import java.util.Collection;

public class ViewBundle {

    private String entry;

    private String viewAngle;

    private Collection<String> contained;


    public ViewBundle(String entry, String viewAngle, Collection<String> contained) {
        this.entry = entry;
        this.viewAngle = viewAngle;
        this.contained = contained;
    }

    public ViewBundle(String entry, String viewAngle) {
        this.entry = entry;
        this.viewAngle = viewAngle;
        this.contained = new ArrayList<>();
        contained.add(entry);
    }

    public String getEntry() {
        return entry;
    }

    public void setEntry(String entry) {
        this.entry = entry;
    }

    public String getViewAngle() {
        return viewAngle;
    }

    public void setViewAngle(String viewAngle) {
        this.viewAngle = viewAngle;
    }

    public Collection<String> getContained() {
        return contained;
    }

    public void setContained(Collection<String> contained) {
        this.contained = contained;
    }
}
