package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;


public class WorkLogPoller implements Closeable {

    private final String driver;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private Logger log = LoggerFactory.getLogger(WorkLogPoller.class);


    /** The pool with data sources for the database connections. */
    private final ComboPooledDataSource connectionPool;


    public WorkLogPoller(String driver, String jdbcUrl, String username, String password) {
        this.driver = driver;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        this.connectionPool = new ComboPooledDataSource();

        silenceC3P0Logger();
        initialiseConnectionPool();
    }

    /**
     * Initialises the ConnectionPool for the connections to the database.
     */
    private void initialiseConnectionPool() {
        try {
            connectionPool.setDriverClass(driver);
            connectionPool.setJdbcUrl(jdbcUrl);
            if (username != null) {
                connectionPool.setUser(username);
            }
            if (password != null) {
                connectionPool.setPassword(password);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Hack to kill com.mchange.v2 log spamming.
     */
    private void silenceC3P0Logger() {
        Properties p = new Properties(System.getProperties());
        p.put("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
        p.put("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF"); // or any other
        System.setProperties(p);
    }

    /**
     * Creates and connects to the database.
     *
     * @return The connection to the database.
     */
    public Connection getConnection() {
        try {
            return connectionPool.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Cleans up after use.
     */
    public void close() {
        try {
            DataSources.destroy(connectionPool);
        } catch (SQLException e) {
            log.error("Could not destroy the connectionPool",e);
        }
    }

    public List<WorkLogUnit> getFedoraEvents(Date since, int limit) throws IOException {

        ArrayList<WorkLogUnit> result = new ArrayList<WorkLogUnit>(limit);

        try {
            Connection conn = getConnection();
            try {
                PreparedStatement statement
                        = conn.prepareStatement("SELECT pid,happened,method,param FROM updateTrackerLogs WHERE " +
                                                "happened >= ? ORDER BY happened ASC LIMIT ?");
                try {
                    statement.setTimestamp(1, new Timestamp(since.getTime()));
                    statement.setInt(2, limit);

                    statement.execute();
                    ResultSet resultSet = statement.getResultSet();
                    while (resultSet.next()) {
                        String pid = resultSet.getString("pid");
                        String method = resultSet.getString("method");
                        String param = resultSet.getString("param");
                        Timestamp timestamp = resultSet.getTimestamp("happened");
                        result.add(new WorkLogUnit(method, timestamp, pid, param));
                    }
                    return result;
                } finally {
                    statement.close();
                }
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }


}
