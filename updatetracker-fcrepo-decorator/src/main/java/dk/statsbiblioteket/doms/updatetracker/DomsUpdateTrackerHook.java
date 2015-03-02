package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.updatetracker.Database.MethodName;
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;

import static dk.statsbiblioteket.doms.updatetracker.Database.MethodName.CREATE;
import static dk.statsbiblioteket.doms.updatetracker.Database.MethodName.DELETE;
import static dk.statsbiblioteket.doms.updatetracker.Database.MethodName.RELATIONS;
import static dk.statsbiblioteket.doms.updatetracker.Database.MethodName.UPDATE;

public class DomsUpdateTrackerHook extends AbstractInvocationHandler implements ModuleConfiguredInvocationHandler {

    /** Logger for this class. */
    private static Log logger = LogFactory.getLog(DomsUpdateTrackerHook.class);

    private Database database;

    @Override
    public void init(Server server) throws InitializationException {
        ConnectionPoolManager cpm = (ConnectionPoolManager) server.getModule("org.fcrepo.server.storage" +
                                                                             ".ConnectionPoolManager");
        if (cpm == null) {
            throw new InitializationException("ConnectionPoolManager module was required, but apparently has " +
                                                    "not been loaded.");
        }

        ManagementModule managementModule = (ManagementModule) server.getModule("org.fcrepo.server.management" +
                                                                                ".Management");
        if (managementModule == null) {
            throw new InitializationException("ManagementModule module was required, but apparently has " +
                                              "not been loaded.");
        }


        String cPoolName = managementModule.getParameter("updateTrackerPoolName");
        ConnectionPool cPool = null;
        try {
            if (cPoolName == null){
                logger.debug("connectionPool unspecified; using default from " + "ConnectionPoolManager.");
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
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        logger.debug("Entering method invoke in FedoraModifyObjectHook with arguments: method='" + method.getName() +
                     "' and arguments: " + Arrays.toString(args));
        //Invoke, then hook
        Object result = method.invoke(target, args);

        Context context = (Context) args[0];
        Date now = Server.getCurrentDate(context);
        String pid = args[1].toString();
        logger.info("The method was called with the pid " + pid);

        String s = method.getName();
        if (s.equals("ingest")) {
            database.addLogEntry(pid, now, CREATE, null);
        } else if (s.equals("modifyObject")) {
            String state = (String) args[2];
            if (state == null) {
                database.addLogEntry(pid, now, UPDATE, null);
            } else {
                if (state.equals("D")) {
                    database.addLogEntry(pid, now, DELETE, null);
                } else {
                    database.addLogEntry(pid, now, MethodName.STATE, state);
                }
            }
        } else if (s.equals("purgeObject")) {
            database.addLogEntry(pid, now, DELETE, null);
        } else if (s.equals("addDatastream") || s.equals("modifyDatastreamByReference") ||
                   s.equals("modifyDatastreamByValue") || s.equals("purgeDatastream") ||
                   s.equals("setDatastreamState") || s.equals("setDatastreamVersionable")) {
            String dsid = (String) args[2];
            if (dsid.equals("RELS-EXT")) {
                database.addLogEntry(pid, now, RELATIONS, null);
            } else {
                database.addLogEntry(pid, now, UPDATE, dsid);
            }
        } else if (s.equals("addRelationship") || s.equals("purgeRelationship")) {
            database.addLogEntry(pid, now, RELATIONS, null);
        } else {
            //Unimportant operation
        }
        return result;
    }
}
