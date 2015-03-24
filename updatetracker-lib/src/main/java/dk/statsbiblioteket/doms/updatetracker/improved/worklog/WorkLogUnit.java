package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import java.util.Date;

/**
 * The java representation of an work unit in the worklog. Each fedora operation (which change something) will
 * be serialised as one of these work units in the database work log.
 */
public class WorkLogUnit {

    private final Long key;
    private final String method;
    private final Date date;
    private final String pid;
    private final String param;

    public WorkLogUnit(Long key, String method, Date date, String pid, String param) {
        this.key = key;
        this.method = method;
        this.date = date;
        this.pid = pid;
        this.param = param;
    }

    public Long getKey() {
        return key;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WorkLogUnit)) {
            return false;
        }

        WorkLogUnit that = (WorkLogUnit) o;

        if (!date.equals(that.date)) {
            return false;
        }
        if (!key.equals(that.key)) {
            return false;
        }
        if (!method.equals(that.method)) {
            return false;
        }
        if (param != null ? !param.equals(that.param) : that.param != null) {
            return false;
        }
        if (!pid.equals(that.pid)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + date.hashCode();
        result = 31 * result + pid.hashCode();
        result = 31 * result + (param != null ? param.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WorkLogUnit{" +
               "key=" + key +
               ", method='" + method + '\'' +
               ", date=" + date +
               ", pid='" + pid + '\'' +
               ", param='" + param + '\'' +
               '}';
    }
}
