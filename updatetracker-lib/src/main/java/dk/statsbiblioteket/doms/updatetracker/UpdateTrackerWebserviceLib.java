package dk.statsbiblioteket.doms.updatetracker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dk.statsbiblioteket.doms.updatetracker.webservice.InvalidCredentialsException;
import dk.statsbiblioteket.doms.updatetracker.webservice.MethodFailedException;
import dk.statsbiblioteket.doms.updatetracker.webservice.PidDatePidPid;
import dk.statsbiblioteket.doms.updatetracker.webservice.UpdateTrackerWebservice;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Update tracker library. Provides upper layers of DOMS with info on changes
 * to objects in Fedora. Used by DOMS Server aka. Central to provide Summa with
 * said info.
 */
public class UpdateTrackerWebserviceLib implements UpdateTrackerWebservice {
    private DateFormat fedoraFormat = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private DateFormat alternativefedoraFormat = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'");
    private CredentialsGenerator credGenerator;
    private final Log log = LogFactory.getLog(getClass());

    public UpdateTrackerWebserviceLib(CredentialsGenerator credGenerator) {
        this.credGenerator = credGenerator;
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
            String collectionPid,
            String viewAngle,
            long beginTime,
            String state,
            Integer offset,
            Integer limit)


            throws InvalidCredentialsException, MethodFailedException

    {

        return getModifiedObjects(collectionPid,
                viewAngle,
                beginTime,
                state,
                offset,
                limit,
                false);
    }

    public List<PidDatePidPid> getModifiedObjects(String collectionPid,
                                                  String viewAngle,
                                                  long beginTime,
                                                  String state,
                                                  Integer offset,
                                                  Integer limit,
                                                  boolean reverse
    )
            throws InvalidCredentialsException, MethodFailedException {
        log.trace("getModifiedObjects called");
        List<PidDatePidPid> result = new ArrayList<PidDatePidPid>();

        List<String> allEntryObjectsInRadioTVCollection;
        Fedora fedora;
        String fedoralocation = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.fedoralocation");
        fedora = new Fedora(credGenerator.getCredentials(), fedoralocation);

        String offsetString;
        if (offset == null) {
            offsetString = " ";
        } else {
            offsetString = " OFFSET " + offset + " ";
        }

        String limitString;
        if (limit == null) {
            limitString = " ";
        } else {
            limitString = " LIMIT " + limit + " ";
        }


        if (state == null) {
            state = "Published";
        }

        String statePrefix = "  FILTER (\n";
        String statePostfix = "  )\n";

        String stateString = "";
        if (state.equals("Published")) {
            stateString = "          ?state =  <info:fedora/fedora-system:def/model#Active> \n";
        } else if (state.equals("InProgress")) {
            stateString = "          ?state =  <info:fedora/fedora-system:def/model#Inactive> \n";
        } else if (state.equals("NotDeleted")) {
            stateString = "          ?state !=  <info:fedora/fedora-system:def/model#Deleted> \n";
        }
        if (!stateString.isEmpty()) {
            stateString = statePrefix + stateString + statePostfix;
        }

        String contentModelFilteringString = "?cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> '"+viewAngle+"' \n";
        /*String contentModelFilteringString = "?cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> ?view \n" +
                " FILTER (\n" +
                " ?view = '"+viewAngle+"'" +
                " )\n";
*/
        String dateSort;
        if (reverse) {
            //TODO remove this when mulgara version is 2.1.5+
            throw new UnsupportedOperationException("reverse is not supported by mulgara 2.1.4");
            //dateSort = " ORDER BY DESC(?date) ";
        } else {
            dateSort = " ORDER BY ?date ";
        }


        String sparql = "SELECT ?object ?cm ?date WHERE {\n" +
                "  ?object <info:fedora/fedora-system:def/model#hasModel> ?cm ;\n" +
                "          <info:fedora/fedora-system:def/view#lastModifiedDate> ?date ;\n" +
                "          <http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection> <info:fedora/" + collectionPid + "> ;\n" +
                "          <info:fedora/fedora-system:def/model#state> ?state .\n" +
                contentModelFilteringString +
                "  FILTER (\n" +
                "    ?date >= '" + fedoraFormat.format(new Date(beginTime)) + "'^^xsd:dateTime\n" +
                "  )\n" +
                stateString +
                "  ?cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> '" + viewAngle + "' .\n" +
                "} " + dateSort + limitString + offsetString;


        log.info("Executing query: '" + sparql + "'");
        try {
            allEntryObjectsInRadioTVCollection
                    = fedora.query(sparql);
        } catch (BackendInvalidCredsException e) {
            throw new InvalidCredentialsException("Invalid credentials", "", e);
        } catch (BackendMethodFailedException e) {
            throw new MethodFailedException("Method failed", "", e);
        }

        log.info("got " + allEntryObjectsInRadioTVCollection.size() + " results.");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        for (String line : allEntryObjectsInRadioTVCollection) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] splitted = line.split(",");
            String pid = splitted[0];
            String entryCMPid = splitted[1];
            String lastModifiedFedoraDate = splitted[2];
            long lastChangedTime;
            try {
                lastModifiedFedoraDate = normalizeFedoraDate(lastModifiedFedoraDate);
                lastChangedTime = dateFormat.parse(lastModifiedFedoraDate).getTime();

            } catch (ParseException e) {
                log.warn("Failed to parse date '" + lastModifiedFedoraDate + "' from object " + splitted[0], e);
                throw new MethodFailedException(
                        "Failed to parse date for object",
                        e.getMessage(),
                        e);
            }
            PidDatePidPid objectThatChanged = new PidDatePidPid();

            objectThatChanged.setPid(pid);
            objectThatChanged.setCollectionPid(collectionPid);
            objectThatChanged.setEntryCMPid(entryCMPid);
            objectThatChanged.setLastChangedTime(lastChangedTime);

            result.add(objectThatChanged);
        }

        log.info("Returning " + result.size() + " records");

        return result;
    }

    private String normalizeFedoraDate(String lastModifiedFedoraDate) {
        if (lastModifiedFedoraDate.matches(".*\\.\\d{3}Z$")) {
            return lastModifiedFedoraDate;
        } else if (lastModifiedFedoraDate.matches(".*\\.\\d{2}Z$")) {
            return lastModifiedFedoraDate.substring(0, lastModifiedFedoraDate.length() - 1) + "0Z";
        } else if (lastModifiedFedoraDate.matches(".*\\.\\d{1}Z$")) {
            return lastModifiedFedoraDate.substring(0, lastModifiedFedoraDate.length() - 1) + "00Z";
        } else if (lastModifiedFedoraDate.matches(".*:\\d\\dZ$")) {
            return lastModifiedFedoraDate.substring(0, lastModifiedFedoraDate.length() - 1) + ".000Z";
        }
        return lastModifiedFedoraDate;
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
            String collectionPid,
            String viewAngle,
            String state)
            throws InvalidCredentialsException, MethodFailedException {

        List<PidDatePidPid> lastChanged = getModifiedObjects(collectionPid,
                viewAngle,
                0,
                state,
                0,
                1,
                true);

        if (!lastChanged.isEmpty()) {
            return lastChanged.get(0).getLastChangedTime();
        } else {
            throw new MethodFailedException("Did not find any elements in the collection", "No elements in the collection");
        }
    }
}
