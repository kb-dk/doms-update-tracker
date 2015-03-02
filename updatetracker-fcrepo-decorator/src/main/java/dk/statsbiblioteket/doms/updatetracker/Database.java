package dk.statsbiblioteket.doms.updatetracker;


import org.fcrepo.server.errors.InitializationException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.utilities.SQLUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class Database implements AutoCloseable {
    public static enum MethodName {
        CREATE,
        UPDATE,
        DELETE,
        STATE,
        RELATIONS
    }

    private static Logger logger = LoggerFactory.getLogger(Database.class);
    private final ConnectionPool cPool;

    public Database(ConnectionPool cPool) throws InitializationException {
        this.cPool = cPool;
        createUpdateTrackingLogTable(cPool);
    }


    /**
     * Create the doIdentifier table
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

    public void addLogEntry(String pid, Date timestamp, MethodName name, String param) throws IOException {

        Connection conn;
        try {
            conn = cPool.getReadWriteConnection();
        } catch (SQLException e) {
            throw new IOException(e);
        }
        try {
            PreparedStatement statement
                    = conn.prepareStatement("INSERT INTO Logs(pid,happened,method,param) VALUES (?,?,?,?)");
            statement.setString(1,pid);
            statement.setTimestamp(2, new Timestamp(timestamp.getTime()));
            statement.setString(3,name.toString());
            statement.setString(4,param);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            cPool.free(conn);
        }
    }

    @Override
    public void close() {
        if (cPool != null) {
            cPool.close();
        }
    }
}