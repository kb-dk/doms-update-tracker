package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.HibernateUtils.set;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.MockUtils.*;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NewspaperTests {


    UpdateTrackerPersistentStore db;
    Fedora fcmock;

    Date beginning = new Date(1);
    protected static final String COLLECTION = "doms:Root_Collection";


    @Before
    public void setUp() throws Exception {
        fcmock = mock(Fedora.class);
        db = new UpdateTrackerPersistentStoreImpl(fcmock);
        db.setUp();
        tearDown();
        db.setUp();

        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(set(COLLECTION));

        //No entry objects or view stuff until initialised
        when(fcmock.getViewInfo(anyString(), any(Date.class))).thenReturn(asList());
    }

    @After
    public void tearDown() throws Exception {
        db.clear();
        db.close();
    }




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
        verifyNoHits(db, SUMMA_VISIBLE);
        verifyNoHits(db, SBOI);
        verifyNoHits(db, GUI);


        //Create the newspaper
        final Date newspaperCreated = createNewspaperObject(db, newspaper, fcmock);
        //Expect to find one summa visible record, the newspaper
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyOneHit(items, newspaper, newspaperCreated);


        //ingest the relevant part of batch
        ingestEdition(db, edition, page1, image1);
        //No items or anything, as no content models added
        verifyNoHits(db, SBOI);
        //Expect to find one summa visible record, the newspaper
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyOneHit(items, newspaper, newspaperCreated);


        //enrich mimetype and relations and content models
        final Date editionGotCM = enrichMimetypesAndRelations(db, edition, page1, image1, fcmock);
        items = db.lookup(beginning, SBOI, 0, 10, null, COLLECTION);
        //We now finds edition as a SBOI hit
        verifyOneHit(items,edition,editionGotCM);
        //In SummaVisible, newspaper is not updated, but page1 now appears
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyTwoHits(items, newspaper, newspaperCreated, page1, editionGotCM);


        //publish batch
        Date editionPublished = publishEdition(db, edition, page1, image1);
        items = db.lookup(beginning, SBOI, 0, 10, ACTIVE, COLLECTION);
        //We now find edition as a SBOI hit, when searching for Active
        verifyOneHit(items, edition, editionPublished);

        //Run the edition maintainer
        final Date maintainerDone = runEditionMaintainer(db, newspaper, edition, page1, fcmock);
        items = db.lookup(beginning, SBOI, 0, 10, ACTIVE, COLLECTION);
        //This updates the edition
        verifyOneHit(items, edition, maintainerDone);

        //In SummaVisible, the page is now updated, but the newspaper is not
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyTwoHits(items, newspaper, newspaperCreated, page1, maintainerDone);


        //We now simulate a change to newspaper
        final Date newspaperChanged = new Date();
        db.datastreamChanged(newspaper, newspaperChanged,"MODS");
        //Both the page and newspaper is updated
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyTwoHits(items,page1,newspaperChanged,newspaper,newspaperChanged);

        //We now simulate a change to edition, which is in the view of page
        final Date editionChanged = new Date();
        db.datastreamChanged(edition, editionChanged, "EDITION");

        //The newspaper have not been updated, but the page have
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyTwoHits(items, newspaper, newspaperChanged, page1, editionChanged);

        //We now simulate the adding of a linked newspaper
        String newspaper2 = "doms:newspaper2";
        final Date newspaper2Created = createNewspaperObject(db, newspaper2, fcmock);
        final Date newspaper2Linked = new Date();
        MockUtils.linkNewspaperToNewspaper(fcmock,edition,page1, newspaper2Linked,newspaper,newspaper2);
        db.objectRelationsChanged(newspaper2,newspaper2Linked);
        //Since the change happened to an unconnected object, it should not have been noticed
        items = db.lookup(beginning, SUMMA_VISIBLE, 0, 10, null, COLLECTION);
        verifyThreeHits(items, newspaper, newspaperChanged, page1, editionChanged, newspaper2, newspaper2Linked);
    }

}
