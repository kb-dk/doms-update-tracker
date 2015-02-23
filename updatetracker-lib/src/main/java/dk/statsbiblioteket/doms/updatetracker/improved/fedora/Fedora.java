package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.FedoraRelation;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.ObjectProfile;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.Views;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.ViewBundle;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewangleType;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewsType;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.util.xml.DOM;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/28/11
 * Time: 12:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class Fedora {

    protected static final String ENTRY_RELATION
            = "http://doms.statsbiblioteket.dk/types/view/default/0/1/#isEntryForViewAngle";
    protected static final String COLLECTION_RELATION
            = "http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection";
    private static Client client = Client.create();
    private final WebResource restApi;
    private final Views views;
    private final TripleStoreRest ts;
    private final FedoraRest fedora;

    //This is not threadsafe so...
    ThreadLocal<Unmarshaller> unmarshaller = new ThreadLocal<Unmarshaller>() {
        @Override
        protected Unmarshaller initialValue() {
            try {
                return JAXBContext.newInstance(ViewsType.class).createUnmarshaller();
            } catch (JAXBException e) {
                throw new RuntimeException(e);
            }
        }
    };


    public Fedora(Credentials creds, String fedoraLocation) throws MalformedURLException {
        restApi = client.resource(fedoraLocation + "/objects/");
        restApi.addFilter(new HTTPBasicAuthFilter(creds.getUsername(), creds.getPassword()));
        fedora = new FedoraRest(creds, fedoraLocation);
        ts = new TripleStoreRest(creds, fedoraLocation, fedora);
        views = new ViewsImpl(ts, fedora);
    }


    public List<ViewInfo> getViewInfo(String pid, Date date) throws FedoraFailedException {


        try {
            Set<String> angles = new HashSet<>();
            Set<String> entryAngles = new HashSet<>();

            ObjectProfile profile = fedora.getLimitedObjectProfile(pid, date.getTime());
            for (String contentmodel : profile.getContentModels()) {
                parseContentModel(contentmodel, date, angles, entryAngles);
            }
            return buildViewInfoList(pid, entryAngles, angles);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException | JAXBException e) {
            throw new FedoraFailedException("Failed to get view info from Fedora for pid " + pid, e);
        }
    }

    private void parseContentModel(String contentmodel, Date date, Set<String> angles, Set<String> entryAngles) throws
                                                           BackendMethodFailedException,
                                                           BackendInvalidCredsException,
                                                           BackendInvalidResourceException,
                                                           JAXBException {

        List<FedoraRelation> entryRelations = fedora.getNamedRelations(contentmodel, ENTRY_RELATION,
                                                                              date.getTime());
        entryRelations.stream().map(FedoraRelation::getObject).forEach(obj -> {
            angles.add(obj);
            entryAngles.add(obj);
        });


        ViewsType viewStream;
        try {
            viewStream = unmarshaller.get().unmarshal(DOM.stringToDOM(fedora.getXMLDatastreamContents(contentmodel,
                                                                                                       "VIEW",
                                                                                                       date.getTime()),
                                                                       true), ViewsType.class).getValue();
        } catch (BackendInvalidResourceException e) {
            return;
            //ignore
        }

        for (ViewangleType viewangleType : viewStream.getViewangle()) {
            String name = viewangleType.getName();
            angles.add(name);
        }
    }




    public ViewBundle calcViewBundle(String entryPid, String viewAngle, Date date) throws FedoraFailedException {
        try {
            List<String> pids = views.getViewObjectsListForObject(entryPid, viewAngle, null);
            return new ViewBundle(entryPid, viewAngle, pids);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed calculating view bundle", e);
        }
    }

    public Set<String> getCollections(java.lang.String pid, Date date) throws FedoraFailedException {
        List<FedoraRelation> collectionRelations = null;
        try {
            collectionRelations = fedora.getNamedRelations(pid, COLLECTION_RELATION,
                                                                                       date.getTime());
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get collection info from Fedora for pid " + pid, e);
        }
        return collectionRelations.stream().map(FedoraRelation::getObject).collect(toSet());
    }



    private List<ViewInfo> buildViewInfoList(String pid, Set<String> entryAngles, Set<String> angles) {
        List<ViewInfo> infoList = new ArrayList<>();
        for (String angle : angles) {
            ViewInfo info = new ViewInfo(angle, pid);
            info.setEntry(entryAngles.contains(angle));
            infoList.add(info);
        }
        return infoList;
    }
}

