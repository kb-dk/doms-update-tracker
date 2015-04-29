package dk.statsbiblioteket.doms.updatetracker.improved.database;

import java.util.ArrayList;
import java.util.List;

public class ViewBundle {

    private String entry;

    private String viewAngle;

    private List<String> contained;


    public ViewBundle(String entry, String viewAngle, List<String> contained) {
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

    public List<String> getContained() {
        return contained;
    }

    public void setContained(List<String> contained) {
        this.contained = contained;
    }
}
