package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import dk.statsbiblioteket.doms.updatetracker.improved.database.Entry;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.webservice.InvalidCredentialsException;
import dk.statsbiblioteket.doms.updatetracker.webservice.MethodFailedException;
import dk.statsbiblioteket.doms.updatetracker.webservice.PidDatePidPid;
import dk.statsbiblioteket.doms.updatetracker.webservice.UpdateTrackerWebservice;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.servlet.http.HttpServletRequest;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Update tracker webservice. Provides upper layers of DOMS with info on changes
 * to objects in Fedora. Used by DOMS Server aka. Central to provide Summa with
 * said info.
 */
@WebService(endpointInterface
                    = "dk.statsbiblioteket.doms.updatetracker.webservice"
                      + ".UpdateTrackerWebservice")
public class UpdateTrackerWebserviceLib implements UpdateTrackerWebservice {

    @Resource
    WebServiceContext context;

    public UpdateTrackerWebserviceLib() throws MethodFailedException {
        try {
            UpdateTrackingSystem.startup();
            //TODO
        } catch (MalformedURLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    /**
     * Lists the entry objects of views (records) in Fedora, in the given
     * collection, that have changed since the given time.
     *
     * @param collectionPid The PID of the collection in which we are looking
     *                      for changes.
     * @param viewAngle     ...TODO doc
     * @param beginTime     The time since which we are looking for changes.
     * @param state         ...TODO doc
     * @return returns java.util.List<dk.statsbiblioteket.doms.updatetracker
     *         .webservice.PidDatePidPid>
     * @throws MethodFailedException
     * @throws InvalidCredentialsException
     */
    public List<PidDatePidPid> listObjectsChangedSince(
            @WebParam(name = "collectionPid", targetNamespace = "")
            String collectionPid,
            @WebParam(name = "viewAngle", targetNamespace = "")
            String viewAngle,
            @WebParam(name = "beginTime", targetNamespace = "")
            long beginTime,
            @WebParam(name = "state", targetNamespace = "")
            String state,
            @WebParam(name = "offset", targetNamespace = "") Integer offset,
            @WebParam(name = "limit", targetNamespace = "") Integer limit)


            throws InvalidCredentialsException, MethodFailedException

    {
        //Filter: state, collection

        List<Entry> entries = null;
        try {
            entries = UpdateTrackingSystem.getStore().lookup(new Date(beginTime), viewAngle, offset, limit, state, collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }
        return convert(entries);

    }

    private List<PidDatePidPid> convert(List<Entry> entries) {
        List<PidDatePidPid> list2 = new ArrayList<PidDatePidPid>(entries.size());
        for (Entry entry : entries) {
            list2.add(convert(entry));
        }
        return list2;
    }

    private PidDatePidPid convert(Entry thing) {
        PidDatePidPid thang = new PidDatePidPid();
        thang.setLastChangedTime(thing.getDateForChange().getTime());
        thang.setPid(thing.getEntryPid());
        return thang;
    }


    /**
     * Return the last time a view/record conforming to the content model of the
     * given content model entry, and in the given collection, has been changed.
     *
     * @param collectionPid The PID of the collection in which we are looking
     *                      for the last change.
     * @param viewAngle     ...TODO doc
     * @return The date/time of the last change.
     * @throws InvalidCredentialsException
     * @throws MethodFailedException
     */
    public long getLatestModificationTime(
            @WebParam(name = "collectionPid", targetNamespace = "")
            String collectionPid,
            @WebParam(name = "viewAngle", targetNamespace = "")
            String viewAngle,
            @WebParam(name = "state", targetNamespace = "")
            String state)
            throws InvalidCredentialsException, MethodFailedException
    {

        List<Entry> entries = null;
        try {
            //TODO no way to control sorting order
            entries = UpdateTrackingSystem.getStore().lookup(new Date(0), viewAngle, 0, 1,state,collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }
        if (entries.size() != 1){
            return 0;
        } else {
            return entries.get(0).getDateForChange().getTime();
        }
    }
}
