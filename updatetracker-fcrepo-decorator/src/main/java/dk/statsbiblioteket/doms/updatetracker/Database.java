package dk.statsbiblioteket.doms.updatetracker;


import org.fcrepo.server.errors.InitializationException;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.utilities.SQLUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;


/**
 * This is the database access object. Use it to add and remove log entries from the update tracker log database
 */
public class Database implements Closeable {


    private static Logger logger = LoggerFactory.getLogger(Database.class);
    private final ConnectionPool cPool;

    public Database(ConnectionPool cPool) throws InitializationException {
        this.cPool = cPool;
        createUpdateTrackingLogTable(cPool);
    }


    /**
     * Create the update tracker log table
     *
     * @param cPool the connection pool
     *
     * @throws org.fcrepo.server.errors.ModuleInitializationException
     */
    private void createUpdateTrackingLogTable(ConnectionPool cPool) throws InitializationException {
    /*Create the table, as this is not created by Fedora default*/
        try {
            String dbSpec = "dk/statsbiblioteket/doms/updatetracker/updateTrackerLogTable.dbspec";
            InputStream specIn = this.getClass()
                                     .getClassLoader()
                                     .getResourceAsStream(dbSpec);
            if (specIn == null) {
                throw new IOException("Cannot find required " + "resource: " +
                                      dbSpec);
            }
            SQLUtility.createNonExistingTables(cPool, specIn);
        } catch (Exception e) {
            throw new InitializationException("Error while attempting to " +
                                              "check for and create non-existing table(s): " +
                                              e.getClass()
                                               .getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Add a log entry to the database
     * @param pid the pid of the object on which the method was invoked
     * @param timestamp the timestamp of the invocation
     * @param name the name of the method
     * @param param the first parameter (after pid) of the method, or null. This will most often be datastream id or state
     * @return the autogenerated key of the log entry in the database
     * @throws IOException
     */
    public Long addLogEntry(String pid, Date timestamp, String name, String param) throws IOException {


        try {
            Connection conn = cPool.getReadWriteConnection();
            try {
                PreparedStatement statement = conn.prepareStatement("INSERT INTO updateTrackerLogs(pid,happened," +
                                                                    "method,param) VALUES (?,?,?,?)",
                                                                    Statement.RETURN_GENERATED_KEYS);
                try {
                    statement.setString(1, pid);
                    statement.setTimestamp(2, new Timestamp(timestamp.getTime()));
                    statement.setString(3, name);
                    statement.setString(4, param);
                    statement.executeUpdate();
                    ResultSet generatedKeys = statement.getGeneratedKeys();
                    generatedKeys.next();
                    long key;
                    key = generatedKeys.getLong("key");

                    return key;
                } finally {
                    statement.close();
                }
            } finally {
                try {
                    conn.close();
                } finally {
                    cPool.free(conn);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to add log entry with with pid='" + pid + "', happened='" +
                                  timestamp.getTime() + "', method='" + name + "', param='" + param +
                                  "' to update tracker database", e);
        }
    }
    public void removeLogEntry(Long key) throws IOException {
        if (key == null || key <= 0) {
            return;
        }

        try {
            Connection conn = cPool.getReadWriteConnection();
            try {
                PreparedStatement statement = conn.prepareStatement("DELETE FROM updateTrackerLogs WHERE key = ?");
                try {
                    statement.setLong(1, key);
                    statement.executeUpdate();
                } finally {
                    statement.close();
                }
            } finally {
                try {
                    conn.close();
                } finally {
                    cPool.free(conn);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to remove log entry with key '"+key+"' from update tracker database",e);
        }
    }

    @Override
    public void close() {
        if (cPool != null) {
            cPool.close();
        }
    }
}