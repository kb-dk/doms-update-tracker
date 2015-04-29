package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.FedoraRelation;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.ObjectProfile;
import dk.statsbiblioteket.doms.central.connectors.fedora.structures.ObjectType;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.EntryAngleCache;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContentModelTest {


    @Before
    public void setUp() throws Exception {
    }

    /**
     * This test that the fedoraForUpdateTracker will correctly check an object for contentmodelness
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     * @throws BackendInvalidResourceException
     * @throws BackendInvalidCredsException
     * @throws BackendMethodFailedException
     */
    @Test
    public void testBecomingContentModel() throws
                                           FedoraFailedException,
                                           UpdateTrackerStorageException,
                                           BackendInvalidResourceException,
                                           BackendInvalidCredsException,
                                           BackendMethodFailedException {
        FedoraRest fedoraRest = mock(FedoraRest.class);
        TripleStoreRest tripleStoreRest = mock(TripleStoreRest.class);
        ViewsImpl views = new ViewsImpl(tripleStoreRest, fedoraRest);
        FedoraForUpdateTracker fedora = new FedoraForUpdateTracker(new EntryAngleCache(), fedoraRest, tripleStoreRest, views);

        final ObjectProfile profile = new ObjectProfile();
        profile.setType(ObjectType.DATA_OBJECT);
        when(fedoraRest.getLimitedObjectProfile(eq("doms:notCurrentlyContentModel1"), anyLong())).thenReturn(profile);

        assertFalse(fedora.isCurrentlyContentModel("doms:notCurrentlyContentModel1", new Date()));
        profile.setType(ObjectType.CONTENT_MODEL);
        assertTrue(fedora.isCurrentlyContentModel("doms:notCurrentlyContentModel1", new Date()));
    }

    /**
     * This tests that the FedoraForUpdateTracker will correctly cache and invalidate content models
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     * @throws BackendInvalidResourceException
     * @throws BackendInvalidCredsException
     * @throws BackendMethodFailedException
     */
    @Test
    public void testCachingViewAngles() throws
                                           FedoraFailedException,
                                           UpdateTrackerStorageException,
                                           BackendInvalidResourceException,
                                           BackendInvalidCredsException,
                                           BackendMethodFailedException {
        FedoraRest fedoraRest = mock(FedoraRest.class);
        TripleStoreRest tripleStoreRest = mock(TripleStoreRest.class);
        ViewsImpl views = new ViewsImpl(tripleStoreRest, fedoraRest);
        FedoraForUpdateTracker fedora = new FedoraForUpdateTracker(new EntryAngleCache(), fedoraRest, tripleStoreRest, views);

        final ObjectProfile cmprofile = new ObjectProfile();
        cmprofile.setType(ObjectType.CONTENT_MODEL);

        when(fedoraRest.getNamedRelations(eq("doms:ContentModel1"),eq(FedoraForUpdateTracker.ENTRY_RELATION), anyLong()))
                .thenReturn(asList(new FedoraRelation("doms:ContentModel1", FedoraForUpdateTracker.ENTRY_RELATION, "SummaVisible")));
        when(fedoraRest.getLimitedObjectProfile(eq("doms:ContentModel1"), anyLong())).thenReturn(cmprofile);
        ObjectProfile objectprofile = new ObjectProfile();
        objectprofile.setContentModels(asList("doms:ContentModel1"));
        when(fedoraRest.getLimitedObjectProfile(eq("doms:Object1"), anyLong())).thenReturn(objectprofile);

        assertTrue(fedora.isCurrentlyContentModel("doms:ContentModel1", new Date()));
        Collection<String> angles = fedora.getEntryAngles("doms:Object1", new Date());
        assertEquals(1,angles.size());
        assertEquals("SummaVisible",angles.iterator().next());

        //No remove the entry relation from the content model
        when(fedoraRest.getNamedRelations(eq("doms:ContentModel1"),
                                          eq(FedoraForUpdateTracker.ENTRY_RELATION),
                                          anyLong())).thenReturn(Collections.<FedoraRelation>emptyList());
        //But this is not visible as we get the cached copy
        angles = fedora.getEntryAngles("doms:Object1", new Date());
        assertEquals(1, angles.size());
        assertEquals("SummaVisible", angles.iterator().next());

        //We now invalidate the cached copy
        fedora.invalidateContentModel("doms:ContentModel1");
        //So no we expect the empty list angles to take effect
        angles = fedora.getEntryAngles("doms:Object1", new Date());
        assertEquals(0, angles.size());


    }



}
