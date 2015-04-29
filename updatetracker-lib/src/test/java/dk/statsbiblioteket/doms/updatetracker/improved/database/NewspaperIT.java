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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.ACTIVE;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.GUI;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.SBOI;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.TestHelpers.SUMMA_VISIBLE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These tests mock up doms, but use the dockerized postgres to test the update tracker with a real database
 */
public class NewspaperIT {


    UpdateTrackerPersistentStore db;
    FedoraForUpdateTracker fcmock;

    Date beginning = new Date(1);
    protected static final String COLLECTION = "doms:Root_Collection";


    @Before
    public void setUp() throws Exception {
        File configFile = new File(Thread.currentThread()
                                         .getContextClassLoader()
                                         .getResource("hibernate.cfg.xml")
                                         .toURI());
        File mappings = new File(Thread.currentThread().getContextClassLoader().getResource("updateTrapperMappings.xml")
                                       .toURI());

        fcmock = mock(FedoraForUpdateTracker.class);
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fcmock,10000L,threadPool);
        db = new UpdateTrackerPersistentStoreImpl(fcmock,
                                                  updateTrackerBackend,
                                                  new DBFactory(configFile,mappings),
                                                  threadPool);
        tearDown();
        db = new UpdateTrackerPersistentStoreImpl(fcmock,
                                                  updateTrackerBackend,
                                                  new DBFactory(configFile, mappings),
                                                  threadPool);

        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(TestHelpers.asSet(COLLECTION));

        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptyList());
    }


    @After
    public void tearDown() throws Exception {
        db.close();
    }


    /**
     * This test tests the process when a newspaper object change. The fixture is most of the code
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    @Test
    public void testUpdateToNewspaper() throws UpdateTrackerStorageException, FedoraFailedException {

        //Editions have relation isPartOfNewspaper, which is a view relation for SummaVisible
        //They are Entry Objects for GUI
        //They are inversely included in the view of EditionPage which is entry for SummaVisible


        //Add a newspaper, with no links
        //Add an edition
        //Add an page, with this edition
        List<Record> items;

        final String newspaper = "doms:newspaper1";
        final String edition = "doms:edition1";
        final String page1 = "doms:page1";
        final String image1 = "doms:image1";


        //All is empty beforehand
        TestHelpers.verifyNoHits(db, SUMMA_VISIBLE);
        TestHelpers.verifyNoHits(db, SBOI);
        TestHelpers.verifyNoHits(db, GUI);


        //Create the newspaper
        final Date newspaperCreated = TestHelpers.createNewspaperObject(db, newspaper, fcmock);
        //Expect to find one summa visible record, the newspaper
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyOneHit(items, newspaper, newspaperCreated);


        //ingest the relevant part of batch
        TestHelpers.ingestEdition(db, edition, page1, image1);
        //No items or anything, as no content models added
        TestHelpers.verifyNoHits(db, SBOI);
        //Expect to find one summa visible record, the newspaper
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyOneHit(items, newspaper, newspaperCreated);


        //enrich mimetype and relations and content models
        final Date editionGotCM = TestHelpers.enrichMimetypesAndRelations(db, edition, page1, image1, fcmock);
        items = db.lookup(beginning, SBOI, 0, 10, null, COLLECTION);
        //We now finds edition as a SBOI hit
        TestHelpers.verifyOneHit(items, edition, editionGotCM);
        //In SummaVisible, newspaper is not updated, but page1 now appears
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyTwoHits(items, newspaper, newspaperCreated, page1, editionGotCM);


        //publish batch
        Date editionPublished = TestHelpers.publishEdition(db, edition, page1, image1);
        items = db.lookup(beginning, SBOI, 0, 10, ACTIVE, COLLECTION);
        //We now find edition as a SBOI hit, when searching for Active
        TestHelpers.verifyOneHit(items, edition, editionPublished);

        //Run the edition maintainer
        final Date maintainerDone = TestHelpers.runEditionMaintainer(db, newspaper, edition, page1, fcmock);
        items = db.lookup(beginning, SBOI, 0, 10, ACTIVE, COLLECTION);
        //This updates the edition
        TestHelpers.verifyOneHit(items, edition, maintainerDone);

        //In SummaVisible, the page is now updated, but the newspaper is not
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyTwoHits(items, newspaper, newspaperCreated, page1, maintainerDone);


        //We now simulate a change to newspaper
        final Date newspaperChanged = new Date();
        db.datastreamChanged(newspaper, newspaperChanged,"MODS", 1);
        //Both the page and newspaper is updated
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyTwoHits(items, newspaper, newspaperChanged, page1, newspaperChanged);

        //We now simulate a change to edition, which is in the view of page
        final Date editionChanged = new Date();
        db.datastreamChanged(edition, editionChanged, "EDITION", 1);

        //The newspaper have not been updated, but the page have
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyTwoHits(items, newspaper, newspaperChanged, page1, editionChanged);

        //We now simulate the adding of a linked newspaper
        String newspaper2 = "doms:newspaper2";
        final Date newspaper2Created = TestHelpers.createNewspaperObject(db, newspaper2, fcmock);
        final Date newspaper2Linked = new Date();
        TestHelpers.linkNewspaperToNewspaper(fcmock, edition, page1, newspaper2Linked, newspaper, newspaper2);
        db.objectRelationsChanged(newspaper2,newspaper2Linked, 1);
        //Since the change happened to an unconnected object, it should not have been noticed
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        TestHelpers.verifyThreeHits(items,
                                    newspaper,
                                    newspaperChanged,
                                    page1,
                                    editionChanged,
                                    newspaper2,
                                    newspaper2Linked);
    }

}
