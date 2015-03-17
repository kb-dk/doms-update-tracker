package dk.statsbiblioteket.doms.updatetracker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fcrepo.server.Context;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ConnectionPoolNotFoundException;
import org.fcrepo.server.errors.InitializationException;
import org.fcrepo.server.management.ManagementModule;
import org.fcrepo.server.proxy.AbstractInvocationHandler;
import org.fcrepo.server.proxy.ModuleConfiguredInvocationHandler;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.storage.ConnectionPoolManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;

/**
 * The update tracker fedora hook. This hook stores information about all changing methods in a database, for later
 * replay in the update tracker.
 * The database is chosen via the fedora ConnectionPoolManager. Set the variable updateTrackerPoolName alongside the
 * decorator to specify which pool should be used.
 * Only changing operations are hooked.
 */
public class DomsUpdateTrackerHook extends AbstractInvocationHandler implements ModuleConfiguredInvocationHandler {

    /** Logger for this class. */
    private static Log logger = LogFactory.getLog(DomsUpdateTrackerHook.class);
    private static Log replayableLog = LogFactory.getLog("dk.statsbiblioteket.doms.updatetracker.ReplayLog");

    private Database database;

    @Override
    public void init(Server server) throws InitializationException {
        ConnectionPoolManager cpm
                = (ConnectionPoolManager) server.getModule("org.fcrepo.server.storage.ConnectionPoolManager");
        if (cpm == null) {
            throw new InitializationException("ConnectionPoolManager module was required, but apparently has not been" +
                                              " loaded.");
        }

        ManagementModule managementModule
                = (ManagementModule) server.getModule("org.fcrepo.server.management.Management");
        if (managementModule == null) {
            throw new InitializationException("ManagementModule module was required, but apparently has not been " +
                                              "loaded.");
        }


        String cPoolName = managementModule.getParameter("updateTrackerPoolName");
        ConnectionPool cPool = null;
        try {
            if (cPoolName == null) {
                logger.debug("connectionPool unspecified; using default from ConnectionPoolManager.");
                cPool = cpm.getPool();
            } else {
                logger.debug("connectionPool specified: " + cPoolName);
                cPool = cpm.getPool(cPoolName);
            }
        } catch (ConnectionPoolNotFoundException e) {
            throw new InitializationException("Failed to find specified connection '" + cPoolName + "'", e);
        }
        try {
            database = new Database(cPool);
        } catch (Exception e) {
            cPool.close();
            final StringWriter out = new StringWriter();
            e.printStackTrace(new PrintWriter(out));
            throw new InitializationException("Failed to open connection: " + out.toString(), e);
        }
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
                                                                     InvocationTargetException,
                                                                     IllegalAccessException {


        final String methodName = method.getName();

        String pid;
        Date now;
        String param;
        try {
            Context context = (Context) args[0];
            now = Server.getCurrentDate(context);
            pid = args[1].toString();
            param = null;
            if (args.length > 2 && args[2] != null) {
                param = args[2].toString();
            }
        } catch (Exception e) {
            final String message = "Failed to parse params for method '" + methodName + "': " + Arrays.toString(args) +
                                   "'";
            logger.error(message, e);
            throw new InvocationTargetException(e, message);
        }

        if (methodName.equals("ingest")) {
            param = null;
            pid = invokeIngestHook(method, args, now);
            replayableLog.info("Method: " + methodName + "(" + pid + ", " + now.getTime() + ", " + param + ")");
            return pid;
        } else if (methodName.equals("modifyObject") || methodName.equals("purgeObject") ||
                   methodName.equals("addDatastream") || methodName.equals("modifyDatastreamByReference") ||
                   methodName.equals("modifyDatastreamByValue") || methodName.equals("purgeDatastream") ||
                   methodName.equals("setDatastreamState") || methodName.equals("setDatastreamVersionable") ||
                   methodName.equals("addRelationship") || methodName.equals("purgeRelationship")) {
            replayableLog.info("Method: " + methodName + "(" + pid + ", " + now.getTime() + ", " + param + ")");
            return invokeHook(method, args, methodName, pid, now, param);
        } else if (methodName.equals("getObjectXML") || methodName.equals("export") ||
                   methodName.equals("getDatastream") || methodName.equals("getDatastreams") ||
                   methodName.equals("getDatastreamHistory") || methodName.equals("putTempStream") ||
                   methodName.equals("getTempStream") || methodName.equals("compareDatastreamChecksum") ||
                   methodName.equals("getNextPID") || methodName.equals("getRelationships") ||
                   methodName.equals("validate")) {
            return method.invoke(target, args);
        } else {
            logger.warn("Unknown method invoked: " + methodName + "(" + pid + ", " + now.getTime() + ", " +
                        param +
                        ")");
            return method.invoke(target, args);
        }
    }

    /**
     * For ingest, we do not know the pid until after the operation completes
     *
     * @param method
     * @param args
     * @param now
     *
     * @return
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private String invokeIngestHook(Method method, Object[] args, Date now) throws
                                                                            InvocationTargetException,
                                                                            IllegalAccessException {
        String methodName = "ingest";

        String pid = (String) method.invoke(target, args);
        addLogEntry(methodName, pid, now, null);
        return pid;
    }

    /**
     * First, add the log entry. Then invoke the method. Should the method fail, remove the log entry
     *
     * @param method     the method
     * @param args       args for the method
     * @param methodName the name of the method
     * @param pid        the pid of the object
     * @param now        timestamp of the invocation
     * @param param      the first param, can be null
     *
     * @return the result of the method invocation
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private Object invokeHook(Method method, Object[] args, String methodName, String pid, Date now,
                              String param) throws IllegalAccessException, InvocationTargetException {
        Long logkey;
        logkey = addLogEntry(methodName, pid, now, param);

        try {
            return method.invoke(target, args);
        } catch (RuntimeException ie) {
            logger.info("Caught exception while invoking method " + methodName + "(" + pid + ", " + now.getTime() +
                        ", " +
                        param +
                        ")" + " . Now attempting to remove log entry from database", ie);

            removeLogEntry(methodName, pid, now, param, logkey);
            throw ie;
        } catch (InvocationTargetException ie) {
            logger.info("Caught exception while invoking method " + methodName + "(" + pid + ", " + now.getTime() +
                        ", " +
                        param +
                        ")" + " . Now attempting to remove log entry from database", ie);

            removeLogEntry(methodName, pid, now, param, logkey);
            throw ie;
        }
    }

    private void removeLogEntry(String methodName, String pid, Date now, String param, Long logkey) {
        try {
            database.removeLogEntry(logkey);

            logger.info("For method" + "" + methodName + "(" + pid + ", " + now.getTime() + ", " +
                        param +
                        ")" + ", we removed logKey '" + logkey + "' from the database");
        } catch (IOException e) {

            logger.error("For method" + "" + methodName + "(" + pid + ", " + now.getTime() + ", " +
                         param +
                         ")" + ", we failed to remove logKey '" + logkey + "' from the database", e);
        }
    }

    private Long addLogEntry(String methodName, String pid, Date now, String param) throws InvocationTargetException {
        Long logkey;
        try {
            logger.debug("For method" + "" + methodName + "(" + pid + ", " + now.getTime() + ", " + param +
                         ")" + ", add a log entry to the database");
            logkey = database.addLogEntry(pid, now, methodName, param);
        } catch (IOException e) {
            final String message = "For method" + "" + methodName + "(" + pid + ", " + now.getTime() + ", " + param +
                                   ")" + ", we failed to add log to database";
            logger.error(message, e);
            throw new InvocationTargetException(e, message);
        }
        return logkey;
    }
}
