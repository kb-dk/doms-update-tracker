package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import dk.statsbiblioteket.doms.updatetracker.improved.database.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.webservice.InvalidCredentialsException;
import dk.statsbiblioteket.doms.updatetracker.webservice.MethodFailedException;
import dk.statsbiblioteket.doms.updatetracker.webservice.PidDatePidPid;
import dk.statsbiblioteket.doms.updatetracker.webservice.UpdateTrackerWebservice;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import java.io.File;
import java.net.MalformedURLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Update tracker webservice. Provides upper layers of DOMS with info on changes
 * to objects in Fedora. Used by DOMS Server aka. Central to provide Summa with
 * said info.
 */
@WebService(endpointInterface
                    = "dk.statsbiblioteket.doms.updatetracker.webservice"
                      + ".UpdateTrackerWebservice")
public class UpdateTrackerWebserviceLib implements UpdateTrackerWebservice {

    private final UpdateTrackingSystem updateTrackingSystem;
    @Resource
    WebServiceContext context;


    public UpdateTrackerWebserviceLib()  {

        UpdateTrackingConfig config = convert(ConfigCollection.getProperties());
        updateTrackingSystem = UpdateTrackingSystem.getInstance(null);

    }

    private UpdateTrackingConfig convert(Properties properties) {

        String fedoraWebUrl = properties.getProperty("fedora.web.URL");
        String fedoraWebUsername = properties.getProperty("fedora.web.Username");
        String fedoraWebPassword = properties.getProperty("fedora.web.Password");

        String fedoraDatabaseDriver = properties.getProperty("fedora.database.driver");
        String fedoraDatabaseURL = properties.getProperty("fedora.database.URL");
        String fedoraDatabaseUsername = properties.getProperty("fedora.database.username");
        String fedoraDatabasePassword = properties.getProperty("fedora.database.password");

        int fedoraUpdatetrackerDelay = Integer.parseInt(properties.getProperty("fedora.updatetracker.delay"));
        int fedoraUpdatetrackerPeriod = Integer.parseInt(properties.getProperty("fedora.updatetracker.period"));
        int fedoraUpdatetrackerLimit = Integer.parseInt(properties.getProperty("fedora.updatetracker.limit"));
        File updatetrackerHibernateConfig = new File(properties.getProperty("fedora.updatetracker.hibernateConfigFile"));

        return new UpdateTrackingConfig(fedoraWebUrl, fedoraWebUsername, fedoraWebPassword, fedoraDatabaseDriver, fedoraDatabaseURL, fedoraDatabaseUsername, fedoraDatabasePassword, fedoraUpdatetrackerDelay, fedoraUpdatetrackerPeriod, fedoraUpdatetrackerLimit, updatetrackerHibernateConfig);
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
            entries = updateTrackingSystem.getStore().lookup(new Date(beginTime), viewAngle, offset, limit, state, collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }
        return convert(entries, state);

    }

    private List<PidDatePidPid> convert(List<Record> entries,String state) {
        List<PidDatePidPid> list2 = new ArrayList<PidDatePidPid>(entries.size());
        for (Record record : entries) {
            list2.add(convert(record, state));
        }
        return list2;
    }

    private PidDatePidPid convert(Record thing, String state) {
        PidDatePidPid thang = new PidDatePidPid();
        switch (state){
            case "A":
                thang.setLastChangedTime(thing.getActive().getTime());
                break;
            case "I":
                thang.setLastChangedTime(thing.getInactive().getTime());
                break;
            case "D":
                thang.setLastChangedTime(thing.getDeleted().getTime());
                break;
            default:
                thang.setLastChangedTime(Math.max(thing.getActive().getTime(),
                                                         Math.max(thing.getInactive().getTime(),
                                                                         thing.getDeleted().getTime())));
                break;
        }

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

        List<Record> entries = null;
        try {
            //TODO no way to control sorting order
            entries = updateTrackingSystem.getStore().lookup(new Date(0), viewAngle, 0, 1,state,collectionPid);
        } catch (UpdateTrackerStorageException e) {
            throw new MethodFailedException("Failed to query the persistent storage","",e);
        }

        Optional<Long> timestampStream = entries.stream().findFirst()
                                                .map(record -> convert(record, state).getLastChangedTime());
        return timestampStream.orElse(0L);

    }
}
