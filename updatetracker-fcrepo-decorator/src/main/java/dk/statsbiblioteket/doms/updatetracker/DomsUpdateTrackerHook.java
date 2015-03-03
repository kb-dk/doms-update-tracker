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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;

/**
 * The update tracker fedora hook. This hook stores information about all changing methods in a database, for later
 * replay in the update tracker.
 * The databse is chosen via the fedora ConnectionPoolManager. Set the variable updateTrackerPoolName alongside the
 * decorator to specify which pool should be used.
 * Only changing operations are hooked, but all operations are logged on info level, as an extra precausion.
 */
public class DomsUpdateTrackerHook extends AbstractInvocationHandler implements ModuleConfiguredInvocationHandler {

    /** Logger for this class. */
    private static Log logger = LogFactory.getLog(DomsUpdateTrackerHook.class);

    private Database database;

    @Override
    public void init(Server server) throws InitializationException {
        ConnectionPoolManager cpm = (ConnectionPoolManager) server.getModule("org.fcrepo.server.storage.ConnectionPoolManager");
        if (cpm == null) {
            throw new InitializationException("ConnectionPoolManager module was required, but apparently has not been loaded.");
        }

        ManagementModule managementModule = (ManagementModule) server.getModule("org.fcrepo.server.management.Management");
        if (managementModule == null) {
            throw new InitializationException("ManagementModule module was required, but apparently has not been loaded.");
        }


        String cPoolName = managementModule.getParameter("updateTrackerPoolName");
        ConnectionPool cPool = null;
        try {
            if (cPoolName == null){
                logger.debug("connectionPool unspecified; using default from ConnectionPoolManager.");
                cPool = cpm.getPool();
            } else {
                logger.debug("connectionPool specified: " + cPoolName);
                cPool = cpm.getPool(cPoolName);
            }
        } catch (ConnectionPoolNotFoundException e) {
            throw new InitializationException("Failed to find specified connection '"+cPoolName+"'",e);
        }
        try {
            database = new Database(cPool);
        } catch (Exception e){
            cPool.close();
            throw new InitializationException("Failed to open connection", e);
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
            if (args.length > 2) {
                param = args[2].toString();
            }
        } catch (Exception e) {
            logger.error("Failed to parse params '" + Arrays.toString(args) + "'", e);
            return method.invoke(proxy, args);
        }

        logger.info("Method: " + methodName + "(" + pid + ", " + now.getTime() + ", " + param + ")");

        switch (methodName) {
            case "ingest":
            case "modifyObject":
            case "purgeObject":
            case "addDatastream":
            case "modifyDatastreamByReference":
            case "modifyDatastreamByValue":
            case "purgeDatastream":
            case "setDatastreamState":
            case "setDatastreamVersionable":
            case "addRelationship":
            case "purgeRelationship":
                return invokeHook(method, args, methodName, pid, now, param);

            case "getObjectXML":
            case "export":
            case "getDatastream":
            case "getDatastreams":
            case "getDatastreamHistory":
            case "putTempStream":
            case "getTempStream":
            case "compareDatastreamChecksum":
            case "getNextPID":
            case "getRelationships":
            case "validate":
                return method.invoke(target, args);

            default:
                logger.warn("Unknown method invoked  '" + methodName + "'");
                return method.invoke(target, args);
        }
    }

    /**
     * First, add the log entry. Then invoke the method. Should the method fail, remove the log entry
     * @param method the method
     * @param args args for the method
     * @param methodName the name of the method
     * @param pid the pid of the object
     * @param now timestamp of the invocation
     * @param param the first param, can be null
     * @return the result of the method invocation
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private Object invokeHook(Method method, Object[] args, String methodName, String pid, Date now,
                              String param) throws IllegalAccessException, InvocationTargetException {
        Long logkey = -1L;
        try {
            logkey = database.addLogEntry(pid, now, methodName, param);
        } catch (IOException e) {
            logger.error("Failed to add log to database",e);
        }

        try {
            return method.invoke(target, args);
        } catch (Throwable tr) {
            try {
                database.removeLogEntry(logkey);
            } catch (IOException e) {
                logger.error("Failed to remove log key '"+logkey+"' from database",e);
            }
            throw tr;
        }
    }
}
