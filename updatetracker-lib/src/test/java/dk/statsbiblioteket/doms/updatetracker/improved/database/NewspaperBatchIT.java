package dk.statsbiblioteket.doms.updatetracker.improved.database;


import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.ALTO;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.EDITION;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.EVENTS;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.FILM;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.MIX;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.MODS;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.SBOI;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These tests mock up doms, but use the dockerized postgres to test the update tracker with a real database
 */
public class NewspaperBatchIT {

    UpdateTrackerPersistentStore db;
    FedoraForUpdateTracker fcmock;



    @Before
    public void setUp() throws Exception {
        fcmock = mock(FedoraForUpdateTracker.class);
        File configFile = new File(Thread.currentThread().getContextClassLoader().getResource("hibernate.cfg.xml")
                                         .toURI());
        File mappings = new File(Thread.currentThread().getContextClassLoader().getResource("updateTrapperMappings.xml")
                                       .toURI());

        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fcmock,10000L);
        db = new UpdateTrackerPersistentStoreImpl(fcmock, updateTrackerBackend,new DBFactory(configFile, mappings));
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

    /**
     * Test that the system correctly handles the process when an Item's Event datastream changes
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Test
    public void testEventsChanged() throws UpdateTrackerStorageException, FedoraFailedException {

        Date beginning = new Date(1);
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(TestHelpers.asSet(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        when(fcmock.getEntryAngles(eq(roundTrip), any(Date.class))).thenReturn(asList(viewAngle));
        db.objectCreated(roundTrip, beginning, 1);

        List<Record> list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        TestHelpers.verifyOneHit(list, roundTrip, beginning);

        Date eventAdded = new Date();
        db.datastreamChanged(roundTrip, eventAdded, EVENTS, 1);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        TestHelpers.verifyOneHit(list, roundTrip, eventAdded);

        Date eventAdded2 = new Date();
        db.datastreamChanged(roundTrip, eventAdded2, EVENTS, 1);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        TestHelpers.verifyOneHit(list, roundTrip, eventAdded2);


        Date eventAdded3 = new Date();
        db.datastreamChanged(roundTrip, eventAdded3, EVENTS, 1);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        TestHelpers.verifyOneHit(list, roundTrip, eventAdded3);
    }


    /**
     * Test that the system correctly handles when an object gets the Content Model Item and thus becomes an record
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     */
    @Test
    public void testBecomingItem() throws FedoraFailedException, UpdateTrackerStorageException {

        Date beginning = new Date(0);
        Date becomingItemTime = new Date();
        Date eventAdded = new Date();
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(TestHelpers.asSet(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        //Not a entry object before this time
        when(fcmock.getEntryAngles(eq(roundTrip), lt(becomingItemTime))).thenReturn(Collections.<String>emptyList());

        //But after this time, the object is an entry
        when(fcmock.getEntryAngles(eq(roundTrip), geq(becomingItemTime))).thenReturn(asList(viewAngle));


        db.objectCreated(roundTrip, beginning, 1);

        //So, no records expected, as no entry objects declared
        List<Record> list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(0, list.size());


        //Even with changes, nothing should be found
        db.datastreamChanged(roundTrip, eventAdded, EVENTS, 1);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(0, list.size());

        //Now we change the relations, at the becomingItem time. So, now the object becomes an entry and is discovered
        db.objectRelationsChanged(roundTrip, becomingItemTime, 1);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(becomingItemTime.getTime(), list.get(0).getDateForChange().getTime());


        //Check that we track changes from this point on
        Date eventAdded3 = new Date();
        db.datastreamChanged(roundTrip, eventAdded3, EVENTS, 1);
        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(eventAdded3.getTime(), list.get(0).getDateForChange().getTime());
    }

    /**
     * Test the entire process a (small) batch goes through, from ingest all the way to enrichment
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     */
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
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(TestHelpers.asSet(collection));

        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptyList());

        //Content Model for roundtrip
        TestHelpers.setContentModelItem(roundTrip, fcmock);

        //Not a entry object before this time

        List<Record> items;
        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(0, items.size());


        //Triggered
        final Date triggerEvents = TestHelpers.batchTriggered(db, batch, roundTrip);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(triggerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());


        //This simulates the order in which objects are created by the doms ingester
        final Date domsIngestEvents = TestHelpers.batchIngested(db, roundTrip, film, edition, page1, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(domsIngestEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());


        // And then the bit repo ingester
        final Date bitRepoEvents = TestHelpers.batchBitRepoIngested(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(bitRepoEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());

        //Jpylyzer
        final Date jpylyzerEvents = TestHelpers.batchJpylyzed(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(jpylyzerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());


        //Histogram
        final Date histograEvents = TestHelpers.batchHistogrammed(db, roundTrip, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(histograEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());


        //Then we come to the enricher
        //Labels
        final Date roundTripLabels = TestHelpers.batchEnriched_Labeled(db,
                                                                       batch,
                                                                       roundTrip,
                                                                       film,
                                                                       edition,
                                                                       page1,
                                                                       image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(roundTripLabels.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(Record.State.INACTIVE, items.get(0).getState());

        //mimetype and relations and content models
        db.datastreamChanged(page1, new Date(), MODS, 1);
        db.datastreamChanged(page1, new Date(), MIX, 1);
        db.datastreamChanged(page1, new Date(), ALTO, 1);
        final Date pageContentModel = new Date();
        TestHelpers.setContentModelPage(page1, pageContentModel, image1, edition, fcmock);
        db.objectRelationsChanged(page1, pageContentModel, 1);

        db.datastreamChanged(edition, new Date(), EDITION, 1);
        final Date editionContentModel = new Date();
        TestHelpers.setContentModelEditionAndItem(edition, editionContentModel, page1, image1, fcmock);
        db.objectRelationsChanged(edition, editionContentModel, 1);

        db.datastreamChanged(film, new Date(), FILM, 1);
        final Date filmContentModel = new Date();
        TestHelpers.setContentModelFilm(film, filmContentModel, edition, page1, image1, fcmock);
        db.objectRelationsChanged(film, filmContentModel, 1);

        final Date roundtripRelations = new Date();
        db.objectRelationsChanged(roundTrip, roundtripRelations, 1);


        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(0).getEntryPid());
        assertEquals(roundTrip, items.get(1).getEntryPid());
        assertEquals(editionContentModel.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(roundtripRelations.getTime(), items.get(1).getDateForChange().getTime());


        //publishing
        TestHelpers.batchPublished(db, roundTrip, film, edition, page1, image1);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(1).getEntryPid());
        assertEquals(roundTrip, items.get(0).getEntryPid());

        final Date enricherEvents = new Date();
        db.datastreamChanged(roundTrip, enricherEvents, EVENTS, 1);

        items = db.lookup(beginning, SBOI, 0, 10, "A", collection);
        assertEquals(2, items.size());
        assertEquals(edition, items.get(0).getEntryPid());
        assertEquals(roundTrip, items.get(1).getEntryPid());
    }
}
