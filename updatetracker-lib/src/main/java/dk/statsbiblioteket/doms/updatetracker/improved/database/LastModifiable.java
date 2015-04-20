package dk.statsbiblioteket.doms.updatetracker.improved.database;

import java.util.Date;

public interface LastModifiable {

    public void setLastModified(Date date);

    public Date getLastModified();
}
