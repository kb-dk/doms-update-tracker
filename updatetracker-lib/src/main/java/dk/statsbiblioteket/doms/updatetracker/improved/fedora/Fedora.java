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
import dk.statsbiblioteket.doms.updatetracker.improved.database.ViewBundle;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.util.Pair;
import dk.statsbiblioteket.util.caching.TimeSensitiveCache;

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

    public static final String ENTRY_RELATION
            = "http://doms.statsbiblioteket.dk/types/view/default/0/1/#isEntryForViewAngle";
    protected static final String COLLECTION_RELATION
            = "http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection";
    protected static final String HASMODEL_RELATION
            = "info:fedora/fedora-system:def/model#hasModel";

    private final Views views;
    private final TripleStoreRest tripleStoreRest;
    private final FedoraRest fedoraRest;
    private final ContentModelCache cmCache;


    private static final int ONE_MINUTE_IN_MILLISECONDS = 60 * 1000;

    /**
     * This is the profile of object profiles. Elements have a lifetime of only one minute, which should prevent the cache
     * from growing to large.
     * As both the pid and the date is part of the key, this is just a cache for multiple invocations during the
     * same event. The next event will have a new date, and will thus not hit the old profile, and thus I do not need
     * to invalidate entries in this cache.
     */
    private static final TimeSensitiveCache<Pair<String,Date>, ObjectProfile> profileCache = new TimeSensitiveCache<>(ONE_MINUTE_IN_MILLISECONDS, false);



    public Fedora(ContentModelCache cmCache, FedoraRest fedoraRest, TripleStoreRest tripleStoreRest, ViewsImpl views) {

        this.cmCache = cmCache;
        this.fedoraRest = fedoraRest;
        this.tripleStoreRest = tripleStoreRest;
        this.views = views;
    }

    public List<String> getEntryAngles(String pid, Date date) throws FedoraFailedException {
        try {
            Set<String> entryAngles = new HashSet<>();
            ObjectProfile profile = getObjectProfile(pid, date);
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
            List<FedoraRelation> entryRelations = fedoraRest.getNamedRelations(contentmodel, ENTRY_RELATION, date.getTime());
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
            collectionRelations = fedoraRest.getNamedRelations(pid, COLLECTION_RELATION,
                                                                                       date.getTime());
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get collection info from Fedora for pid " + pid, e);
        }
        return collectionRelations.stream().map(FedoraRelation::getObject).collect(toSet());
    }

    public Set<String> getObjectsOfThisContentModel(String contentModelPid) throws FedoraFailedException {

        List<FedoraRelation> hasModelRelations;
        try {
            hasModelRelations = tripleStoreRest.getInverseRelations(contentModelPid,HASMODEL_RELATION);
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new FedoraFailedException("Failed to get content model info from Fedora for pid " + contentModelPid, e);
        }
        return hasModelRelations.stream()
                                  .map(FedoraRelation::getSubject)
                                  .collect(toSet());
    }

    public boolean isCurrentlyContentModel(String pid, Date date) throws FedoraFailedException {
        if (cmCache.isCachedContentModel(pid)){
            return true;
        }
        try {
            ObjectProfile profile = getObjectProfile(pid, date);
            return profile.getType() == ObjectType.CONTENT_MODEL;
        } catch (BackendInvalidCredsException | BackendInvalidResourceException | BackendMethodFailedException e) {
            throw new FedoraFailedException("Failed to get profile of object '"+pid+"' at date '"+date.toString()+"'",e);
        }
    }

    private ObjectProfile getObjectProfile(String pid, Date date) throws
                                                                  BackendInvalidResourceException,
                                                                  BackendMethodFailedException,
                                                                  BackendInvalidCredsException {
        final Pair<String, Date> key = new Pair<>(pid, date);
        ObjectProfile cachedProfile = profileCache.get(key);
        if (cachedProfile == null){
            cachedProfile = fedoraRest.getLimitedObjectProfile(pid, date.getTime());
            profileCache.put(key,cachedProfile);
        }
        return cachedProfile;
    }

    public void invalidateContentModel(String pid) {
        cmCache.invalidateContentModel(pid);
    }
}

