package dk.statsbiblioteket.doms.updatetracker.webservice;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import dk.statsbiblioteket.doms.updatetracker.improved.database.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;

import javax.annotation.Resource;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import java.io.File;
import java.lang.*;
import java.lang.String;
import java.util.*;
import java.util.Date;

/**
 * Update tracker webservice. Provides upper layers of DOMS with info on changes
 * to objects in Fedora. Used by DOMS Server aka. Central to provide Summa with
 * said info.
 */
@WebService(endpointInterface
        = "dk.statsbiblioteket.doms.updatetracker.webservice"
        + ".UpdateTrackerWebservice")
public class UpdateTrackerWebserviceImpl implements UpdateTrackerWebservice {

    private final UpdateTrackingSystem updateTrackingSystem;
    @Resource
    WebServiceContext context;

    public UpdateTrackerWebserviceImpl() throws MethodFailedException {

        UpdateTrackingConfig config = new UpdateTrackingConfig(ConfigCollection.getProperties());
        updateTrackingSystem = UpdateTrackingSystem.getInstance(config);
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

        List<Record> entries = null;
        try {
            entries = updateTrackingSystem.getStore().lookup(new java.util.Date(beginTime),
                                                                                     viewAngle,
                                                                                     offset,
                                                                                     limit,
                                                                                     state,
                                                                                     collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }
        return convert(entries, state);
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
            java.lang.String collectionPid,
            @WebParam(name = "viewAngle", targetNamespace = "")
            java.lang.String viewAngle,
            @WebParam(name = "state", targetNamespace = "")
            java.lang.String state)
            throws InvalidCredentialsException, MethodFailedException
    {

        List<Record> entries = null;
        try {
            //TODO no way to control sorting order
            entries = updateTrackingSystem.getStore().lookup(new Date(0),
                                                                                     viewAngle, 0, 1,
                                                                                     state,
                                                                                     collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }

        Optional<java.lang.Long> timestampStream = entries.stream().findFirst()
                                                .map(record -> convert(record, state).getLastChangedTime());
        return timestampStream.orElse(0L);
    }


    private List<PidDatePidPid> convert(List<Record> entries, String state) {
        List<PidDatePidPid> list2 = new ArrayList<PidDatePidPid>(entries.size());
        for (Record record : entries) {
            list2.add(convert(record, state));
        }
        return list2;
    }

    private PidDatePidPid convert(Record thing, String state) {
        PidDatePidPid thang = new PidDatePidPid();
        switch (state) {
            case "A":
                thang.setLastChangedTime(thing.getActive()
                                              .getTime());
                break;
            case "I":
                thang.setLastChangedTime(thing.getInactive()
                                              .getTime());
                break;
            case "D":
                thang.setLastChangedTime(thing.getDeleted()
                                              .getTime());
                break;
            default:
                thang.setLastChangedTime(Math.max(thing.getActive()
                                                       .getTime(), Math.max(thing.getInactive()
                                                                                 .getTime(),
                                                                            thing.getDeleted()
                                                                                 .getTime())));
                break;
        }

        thang.setPid(thing.getEntryPid());
        return thang;
    }
}
