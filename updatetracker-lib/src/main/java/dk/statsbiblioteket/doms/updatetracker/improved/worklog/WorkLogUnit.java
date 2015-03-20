package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import java.util.Date;

/**
 * The java representation of an work unit in the worklog. Each fedora operation (which change something) will
 * be serialised as one of these work units in the database work log.
 */
public class WorkLogUnit {

    private String method;

    private Date date;

    private String pid;

    private String param;

    public WorkLogUnit(String method, Date date, String pid, String param) {
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

    @Override
    public String toString() {
        return "WorkLogUnit{" +
               "method='" + method + '\'' +
               ", date=" + date +
               ", pid='" + pid + '\'' +
               ", param='" + param + '\'' +
               '}';
    }
}
