package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.FedoraRelation;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.ObjectProfile;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.ObjectType;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.Views;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.ContentModelCache;
import dk.statsbiblioteket.doms.updatetracker.improved.database.ViewBundle;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import javax.xml.bind.JAXBException;
import java.lang.String;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * This class embeds the specific functions we need for Fedora in the update tracker
 */
public class Fedora {

    protected static final String ENTRY_RELATION
            = "http://doms.statsbiblioteket.dk/types/view/default/0/1/#isEntryForViewAngle";
    protected static final String COLLECTION_RELATION
            = "http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection";
    protected static final String HASMODEL_RELATION
            = "info:fedora/fedora-system:def/model#hasModel";

    private static Client client = Client.create();
    private final Views views;
    private final TripleStoreRest ts;
    private final FedoraRest fedora;
    private final ContentModelCache cmCache;

    public Fedora(Credentials creds, String fedoraLocation) throws MalformedURLException {
        this.cmCache = new ContentModelCache();
        WebResource restApi = client.resource(fedoraLocation + "/objects/");
        restApi.addFilter(new HTTPBasicAuthFilter(creds.getUsername(), creds.getPassword()));
        fedora = new FedoraRest(creds, fedoraLocation);
        ts = new TripleStoreRest(creds, fedoraLocation, fedora);
        views = new ViewsImpl(ts, fedora);
    }

    public List<String> getEntryAngles(String pid, Date date) throws FedoraFailedException {
        try {
            Set<String> entryAngles = new HashSet<>();
            ObjectProfile profile = fedora.getLimitedObjectProfile(pid, date.getTime());
            if (profile.getType() == ObjectType.CONTENT_MODEL){
                cmCache.setEntryViewAngles(pid, getEntryAnglesForContentModel(pid, date) );
            }
            for (String contentmodelPid : profile.getContentModels()) {
                entryAngles.addAll(getEntryAnglesForContentModel(contentmodelPid, date));
            }
            return new ArrayList<>(entryAngles);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException | JAXBException e) {
            throw new FedoraFailedException("Failed to get view info from Fedora for pid " + pid, e);
        }
    }

    private Set<String> getEntryAnglesForContentModel(String contentmodel, Date date) throws
                                                           BackendMethodFailedException,
                                                           BackendInvalidCredsException,
                                                           BackendInvalidResourceException,
                                                           JAXBException {
        Set<String> entryAngles = cmCache.getCachedEntryAngles(contentmodel);
        if (entryAngles == null) {
            List<FedoraRelation> entryRelations = fedora.getNamedRelations(contentmodel, ENTRY_RELATION, date.getTime());
            entryAngles = entryRelations.stream()
                                 .map(FedoraRelation::getObject)
                                 .collect(toSet());
            cmCache.setEntryViewAngles(contentmodel,entryAngles);
        }
        return entryAngles;
   }

    public ViewBundle calcViewBundle(String entryPid, String viewAngle, Date date) throws FedoraFailedException {
        try {
            List<String> pids = views.getViewObjectsListForObject(entryPid, viewAngle, date.getTime());
            return new ViewBundle(entryPid, viewAngle, pids);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed calculating view bundle", e);
        }
    }

    public Set<String> getCollections(String pid, Date date) throws FedoraFailedException {
        List<FedoraRelation> collectionRelations = null;
        try {
            collectionRelations = fedora.getNamedRelations(pid, COLLECTION_RELATION,
                                                                                       date.getTime());
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get collection info from Fedora for pid " + pid, e);
        }
        return collectionRelations.stream().map(FedoraRelation::getObject).collect(toSet());
    }

    public Set<String> getObjectsOfThisContentModel(String contentModelPid) throws FedoraFailedException {

        List<FedoraRelation> hasModelRelations;
        try {
            hasModelRelations = ts.getInverseRelations(contentModelPid,HASMODEL_RELATION);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get content model info from Fedora for pid " + contentModelPid, e);
        }
        return hasModelRelations.stream()
                                  .map(FedoraRelation::getSubject)
                                  .collect(toSet());
    }

    public boolean isContentModel(String pid) {
        //TODO if not cached, perform check
        return cmCache.isCachedContentModel(pid);
    }

    public void invalidateContentModel(String pid) {
        cmCache.invalidateContentModel(pid);
    }
}

