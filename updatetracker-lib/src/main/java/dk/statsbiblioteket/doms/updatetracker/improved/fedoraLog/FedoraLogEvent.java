package dk.statsbiblioteket.doms.updatetracker.improved.fedoraLog;

import java.util.Date;

public class FedoraLogEvent {

    private String method;

    private Date date;

    private String pid;

    private String param;

    public FedoraLogEvent(String method, Date date, String pid, String param) {
        this.method = method;
        this.date = date;
        this.pid = pid;
        this.param = param;
    }

    public String getMethod() {
        return method;
    }

    public Date getDate() {
        return date;
    }

    public String getPid() {
        return pid;
    }

    public String getParam() {
        return param;
    }
}
