package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.inheritance.ContentModelInheritance;
import dk.statsbiblioteket.doms.central.connectors.fedora.inheritance.ContentModelInheritanceImpl;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStore;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.Views;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.ViewBundle;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ObjectProfile;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewangleType;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewsType;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/28/11
 * Time: 12:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class Fedora {

    private static Client client = Client.create();
    private final WebResource restApi;
    private final Views views;

    public Fedora(Credentials creds, String fedoraLocation)
            throws MalformedURLException {
        restApi = client.resource(fedoraLocation + "/objects/");
        restApi.addFilter(new HTTPBasicAuthFilter(creds.getUsername(), creds.getPassword()));
        dk.statsbiblioteket.doms.central.connectors.fedora.Fedora fedora = new FedoraRest(creds, fedoraLocation);
        TripleStore ts = new TripleStoreRest(creds, fedoraLocation, fedora);
        ContentModelInheritance inheritance= new ContentModelInheritanceImpl(fedora, ts);

        views = new ViewsImpl(ts, inheritance, fedora);

    }


    public List<ViewInfo> getViewInfo(String pid, Date date) throws FedoraFailedException {

        Map<String, List<String>> relations = new HashMap<>();
        Map<String, List<String>> inverseRelations = new HashMap<>();

        Set<String> entryAngles;
        try {
            entryAngles = views.determineEntryAngles(pid, date.getTime());
        } catch (BackendInvalidCredsException|BackendMethodFailedException|BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get view info from Fedora for pid " + pid, e);
        }

        Set<String> angles = new HashSet<>();
        angles.addAll(entryAngles);

        ObjectProfile profile;
        try {
            profile = restApi.path(pid)
                    .queryParam("asOfDateTime", DateUtility.convertDateToString(date))
                    .queryParam("format", "xml")
                    .get(ObjectProfile.class);
        } catch (UniformInterfaceException e) {
            throw new FedoraFailedException("Failed to query fedora", e);
        }


        List<String> contentmodels = profile.getObjModels().getModel();


        for (String contentmodel : contentmodels) {
            ViewsType viewStream;
            try {
                contentmodel = contentmodel.replaceAll("info:fedora/","");
                viewStream = restApi.path(contentmodel).path("/datastreams/VIEW/content")
                        .queryParam("asOfDateTime", DateUtility.convertDateToString(date))
                        .get(ViewsType.class);
            } catch (UniformInterfaceException e) {
                if (e.getResponse().getStatusInfo().getStatusCode() != 404 ){
                    throw new FedoraFailedException("Failed to query fedora", e);
                } else {
                    continue;
                }
            }

            for (ViewangleType viewangleType : viewStream.getViewangle()) {
                String name = viewangleType.getName();
                angles.add(name);

                List<String> rels = relations.get(name);
                if (rels == null) {
                    rels = new ArrayList<>();
                }
                List<Object> markedRels = viewangleType.getRelations().getAny();
                for (Object markedRel : markedRels) {
                    rels.add(markedRel.toString());
                }
                relations.put(name, rels);

                List<String> invrels = relations.get(name);
                if (invrels == null) {
                    invrels = new ArrayList<>();
                }
                List<Object> markedInvRels = viewangleType.getInverseRelations().getAny();
                for (Object markedInvRel : markedInvRels) {
                    invrels.add(markedInvRel.toString());
                }
                inverseRelations.put(name, rels);
            }
        }

        List<ViewInfo> infoList = new ArrayList<>();
        for (String angle : angles) {
            ViewInfo info = new ViewInfo(angle, pid);
            info.setEntry(entryAngles.contains(angle));
            info.setRelations(relations.get(angle));
            info.setInverseRelations(inverseRelations.get(angle));
            infoList.add(info);
        }
        return infoList;


    }

    public ViewBundle calcViewBundle(String entryPid, String viewAngle, Date date)
            throws BackendMethodFailedException, BackendInvalidResourceException, BackendInvalidCredsException {


        List<String> pids = views.getViewObjectsListForObject(entryPid, viewAngle, null);
        return new ViewBundle(entryPid,viewAngle,pids);
    }
}

