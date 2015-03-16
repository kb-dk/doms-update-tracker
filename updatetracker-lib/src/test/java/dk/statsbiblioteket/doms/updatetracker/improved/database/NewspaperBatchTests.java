package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerDAO.asSet;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.ALTO;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.EDITION;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.EVENTS;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.FILM;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.MIX;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.MODS;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.Tests.SBOI;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static junit.framework.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NewspaperBatchTests {

    private final File configFile;
    UpdateTrackerPersistentStore db;
    FedoraForUpdateTracker fcmock;

    public NewspaperBatchTests() throws URISyntaxException {
        fcmock = mock(FedoraForUpdateTracker.class);
        configFile = new File(Thread.currentThread().getContextClassLoader().getResource("hibernate.cfg.xml").toURI());
    }


    @Before
    public void setUp() throws Exception {
        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fcmock);
        db = new UpdateTrackerPersistentStoreImpl(configFile,fcmock, updateTrackerBackend);
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

    @Test
    public void testEventsChanged() throws UpdateTrackerStorageException, FedoraFailedException {

        Date beginning = new Date(1);
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(asSet(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        when(fcmock.getEntryAngles(eq(roundTrip), any(Date.class))).thenReturn(asList(viewAngle));
        db.objectCreated(roundTrip, beginning);

        List<Record> list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        Tests.verifyOneHit(list, roundTrip, beginning);

        Date eventAdded = new Date();
        db.datastreamChanged(roundTrip, eventAdded, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        Tests.verifyOneHit(list, roundTrip, eventAdded);

        Date eventAdded2 = new Date();
        db.datastreamChanged(roundTrip, eventAdded2, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        Tests.verifyOneHit(list, roundTrip, eventAdded2);


        Date eventAdded3 = new Date();
        db.datastreamChanged(roundTrip, eventAdded3, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        Tests.verifyOneHit(list, roundTrip, eventAdded3);
    }


    @Test
    public void testBecomingItem() throws FedoraFailedException, UpdateTrackerStorageException {

        Date beginning = new Date(0);
        Date becomingItemTime = new Date();
        Date eventAdded = new Date();
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(asSet(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        //Not a entry object before this time
        when(fcmock.getEntryAngles(eq(roundTrip), lt(becomingItemTime))).thenReturn(Collections.<String>emptyList());

        //But after this time, the object is an entry
        when(fcmock.getEntryAngles(eq(roundTrip), geq(becomingItemTime))).thenReturn(asList(viewAngle));


        db.objectCreated(roundTrip, beginning);

        //So, no records expected, as no entry objects declared
        List<Record> list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(0, list.size());


        //Even with changes, nothing should be found
        db.datastreamChanged(roundTrip, eventAdded, EVENTS);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(0, list.size());

        //Now we change the relations, at the becomingItem time. So, now the object becomes an entry and is discovered
        db.objectRelationsChanged(roundTrip, becomingItemTime);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(becomingItemTime.getTime(), list.get(0).getDateForChange().getTime());


        //Check that we track changes from this point on
        Date eventAdded3 = new Date();
        db.datastreamChanged(roundTrip, eventAdded3, EVENTS);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(eventAdded3.getTime(), list.get(0).getDateForChange().getTime());
    }

    @Test
    public void testIngestToEnrich() throws FedoraFailedException, UpdateTrackerStorageException {

        //TODO test for the other view angles
        Date beginning = new Date(1);

        final String batch = "doms:batch1";
        final String roundTrip = "doms:roundTrip1";
        final String film = "doms:film1";
        final String edition = "doms:edition1";
        final String page1 = "doms:page1";
        final String image1 = "doms:image1";
        final String collection = "doms:Root_Collection";

        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(asSet(collection));

        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptyList());

        //Content Model for roundtrip
        Tests.setContentModelItem(roundTrip, fcmock);

        //Not a entry object before this time

        List<Record> items;
        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(0, items.size());


        //Triggered
        final Date triggerEvents = Tests.batchTriggered(db, batch, roundTrip);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(triggerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //This simulates the order in which objects are created by the doms ingester
        final Date domsIngestEvents = Tests.batchIngested(db, roundTrip, film, edition, page1, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(domsIngestEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        // And then the bit repo ingester
        final Date bitRepoEvents = Tests.batchBitRepoIngested(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(bitRepoEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());

        //Jpylyzer
        final Date jpylyzerEvents = Tests.batchJpylyzed(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(jpylyzerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //Histogram
        final Date histograEvents = Tests.batchHistogrammed(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(histograEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //Then we come to the enricher
        //Labels
        final Date roundTripLabels = Tests.batchEnriched_Labeled(db, batch, roundTrip, film, edition, page1, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(roundTripLabels.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());

        //mimetype and relations and content models
        db.datastreamChanged(page1, new Date(), MODS);
        db.datastreamChanged(page1, new Date(), MIX);
        db.datastreamChanged(page1, new Date(), ALTO);
        final Date pageContentModel = new Date();
        Tests.setContentModelPage(page1, pageContentModel, image1, edition, fcmock);
        db.objectRelationsChanged(page1, pageContentModel);

        db.datastreamChanged(edition, new Date(), EDITION);
        final Date editionContentModel = new Date();
        Tests.setContentModelEditionAndItem(edition, editionContentModel, page1, image1, fcmock);
        db.objectRelationsChanged(edition, editionContentModel);

        db.datastreamChanged(film, new Date(), FILM);
        final Date filmContentModel = new Date();
        Tests.setContentModelFilm(film, filmContentModel, edition, page1, image1, fcmock);
        db.objectRelationsChanged(film, filmContentModel);

        final Date roundtripRelations = new Date();
        db.objectRelationsChanged(roundTrip, roundtripRelations);


        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(0).getEntryPid());
        assertEquals(roundTrip, items.get(1).getEntryPid());
        assertEquals(editionContentModel.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(roundtripRelations.getTime(), items.get(1).getDateForChange().getTime());


        //publishing
        Tests.batchPublished(db, roundTrip, film, edition, page1, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(1).getEntryPid());
        assertEquals(roundTrip, items.get(0).getEntryPid());

        final Date enricherEvents = new Date();
        db.datastreamChanged(roundTrip, enricherEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, "A", collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(0).getEntryPid());
        assertEquals(roundTrip, items.get(1).getEntryPid());
    }
}
