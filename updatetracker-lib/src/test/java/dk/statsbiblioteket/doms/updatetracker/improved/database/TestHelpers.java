package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class TestHelpers {
    protected static final String SBOI = "SBOI";
    protected static final String GUI = "GUI";
    protected static final String SUMMA_VISIBLE = "SummaVisible";
    protected static final String INACTIVE = "I";
    protected static final String ACTIVE = "A";
    protected static final String EVENTS = "EVENTS";
    protected static final String FILM = "FILM";
    protected static final String EDITION = "EDITION";
    protected static final String MODS = "MODS";
    protected static final String MIX = "MIX";
    protected static final String ALTO = "ALTO";
    protected static final String CONTENT = "CONTENT";
    protected static final String RELS_INT = "RELS-INT";
    protected static final String JPYLYZER = "JPYLYZER";
    protected static final String HISTOGRAM = "HISTOGRAM";

    /**
     * Set the ContentModel_Item for the roundtrip object
     * @param roundTrip
     * @param fcmock
     * @throws FedoraFailedException
     */
    static void setContentModelItem(String roundTrip, FedoraForUpdateTracker fcmock) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(roundTrip), eq(SBOI), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                        SBOI));
        when(fcmock.getEntryAngles(eq(roundTrip), gt(new Date()))).thenReturn(asList(SBOI));
    }

    /**
     * Set the content model film for the given object. This content model specifies that all the arguments are part of the GUI view
     * @param pid
     * @param date from when the content model should count
     * @param edition
     * @param page
     * @param image
     * @param fcmock
     * @throws FedoraFailedException
     */
    static void setContentModelFilm(String pid, Date date, String edition, String page, String image,
                                    FedoraForUpdateTracker fcmock) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(date))).thenReturn(new ViewBundle(pid, GUI, asList(pid,
                                                                                                            edition,
                                                                                                            page,
                                                                                                            image)));
    }

    /**
     * Set the content model item and edition on an edition.
     * This sets edition,page,image as an entry record in GUI,
     * This sets edition as an record for SummaVisible
     * This sets edition as an record for SBOI
     * @param pid
     * @param date
     * @param page
     * @param image
     * @param fcmock
     * @throws FedoraFailedException
     */
    static void setContentModelEditionAndItem(String pid, Date date, String page, String image, FedoraForUpdateTracker fcmock) throws
                                                                                                               FedoraFailedException {

        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(date))).thenReturn(new ViewBundle(pid, GUI, asList(pid,
                                                                                                            page,
                                                                                                            image)));

        when(fcmock.calcViewBundle(eq(pid), eq(SUMMA_VISIBLE), geq(date))).thenReturn(new ViewBundle(pid,
                                                                                                     SUMMA_VISIBLE,
                                                                                                     asList(pid)));
        when(fcmock.calcViewBundle(eq(pid), eq(SBOI), geq(date))).thenReturn(new ViewBundle(pid, SBOI, asList(pid)));


        when(fcmock.getEntryAngles(eq(pid), geq(date))).thenReturn(asList(GUI, SBOI));
    }

    /**
     * This sets the content model page
     * Page is an entry for SummaVisible, and includes page and edition
     * Page is not an entry for GUI, but includes page and image
     * @param pid
     * @param pageContentModel
     * @param image
     * @param edition
     * @param fcmock
     * @throws FedoraFailedException
     */
    static void setContentModelPage(String pid, Date pageContentModel, String image, String edition,
                                    FedoraForUpdateTracker fcmock) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(pageContentModel))).thenReturn(new ViewBundle(pid,
                                                                                                       GUI,
                                                                                                       asList(pid,
                                                                                                              image)));
        when(fcmock.calcViewBundle(eq(pid), eq(SUMMA_VISIBLE), geq(pageContentModel))).thenReturn(new ViewBundle(pid,
                                                                                                                 SUMMA_VISIBLE,
                                                                                                                 asList(pid,
                                                                                                                        edition)));

        //But after this time, the object is an entry
        when(fcmock.getEntryAngles(eq(pid), geq(pageContentModel))).thenReturn(asList(SUMMA_VISIBLE));
    }

    /**
     * This sets the content model newspaper
     * Newspaper is an entry for SummaVisible and GUI, and includes nothing else for both
     * @param pid
     * @param changeDate
     * @param fcmock
     * @throws FedoraFailedException
     */
    public static void setContentModelNewspaper(String pid, Date changeDate, FedoraForUpdateTracker fcmock) throws
                                                                                            FedoraFailedException {

        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(changeDate))).thenReturn(new ViewBundle(pid, GUI));
        when(fcmock.calcViewBundle(eq(pid), eq(SUMMA_VISIBLE), geq(changeDate))).thenReturn(new ViewBundle(pid,
                                                                                                           SUMMA_VISIBLE));

        //But after this time, the object is an entry
        when(fcmock.getEntryAngles(eq(pid), geq(changeDate))).thenReturn(asList(SUMMA_VISIBLE, GUI));


        //Newspapers are a linked list, with the relation relatedSucceeding. These are view relations in
        // SummaAuthority and SummaVisible
        //It is a entry for SummaVisible and GUI


    }


    /**
     * Link an edition to a newspaper
     * Page now includes newspaper and edition in SummaVisible
     * Edition now includes newspaer in SummaVisible
     * @param fcmock
     * @param page1
     * @param editionLinkedToNewspaper
     * @param edition
     * @param newspaper
     * @throws FedoraFailedException
     */
    public static void linkEditionToNewspaper(FedoraForUpdateTracker fcmock, String page1, Date editionLinkedToNewspaper,
                                              String edition, String newspaper) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(edition), eq(SUMMA_VISIBLE), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(edition,
                                                                                                         SUMMA_VISIBLE,
                                                                                                         asList(edition,
                                                                                                                newspaper)));

        when(fcmock.calcViewBundle(eq(page1), eq(SUMMA_VISIBLE), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(page1,
                                                                                                                 SUMMA_VISIBLE,
                                                                                                                 asList(page1,
                                                                                                                        edition,newspaper)));

    }

    public static void linkNewspaperToNewspaper(FedoraForUpdateTracker fcmock, String edition, String page1,
                                                Date editionLinkedToNewspaper, String newspaper, String newspaper2) throws
                                                                                                 FedoraFailedException {
        when(fcmock.calcViewBundle(eq(edition), eq(SUMMA_VISIBLE), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(edition,
                                                                                                                             SUMMA_VISIBLE,
                                                                                                                             asList(edition,
                                                                                                                                    newspaper,
                                                                                                                                    newspaper2)));

        when(fcmock.calcViewBundle(eq(page1), eq(SUMMA_VISIBLE), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(page1,
                                                                                                                           SUMMA_VISIBLE,
                                                                                                                           asList(page1,
                                                                                                                                  edition,
                                                                                                                                  newspaper,
                                                                                                                                  newspaper2)));
        when(fcmock.calcViewBundle(eq(newspaper), eq(GUI), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(newspaper, GUI,

                                                                                                                     Arrays.asList(newspaper,newspaper2)));
        when(fcmock.calcViewBundle(eq(newspaper), eq(SUMMA_VISIBLE), geq(editionLinkedToNewspaper))).thenReturn(new ViewBundle(newspaper,
                                                                                                           SUMMA_VISIBLE, Arrays.asList(newspaper,newspaper2)));

    }


    /**
     * The events happening when a batch is published
     * @param db
     * @param roundTrip
     * @param film
     * @param edition
     * @param page1
     * @param image1
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static void batchPublished(UpdateTrackerPersistentStore db, String roundTrip, String film, String edition,
                               String page1, String image1) throws
                                                                                                            UpdateTrackerStorageException,
                                                                                                            FedoraFailedException {
        db.objectStateChanged(roundTrip, new Date(), ACTIVE, 1);
        db.objectStateChanged(film, new Date(), ACTIVE, 1);
        db.objectStateChanged(edition, new Date(), ACTIVE, 1);
        db.objectStateChanged(page1, new Date(), ACTIVE, 1);
        db.objectStateChanged(image1, new Date(), ACTIVE, 1);
    }

    /**
     * The events happening when a batch have its labels changed
     * @param db
     * @param batch
     * @param roundTrip
     * @param film
     * @param edition
     * @param page1
     * @param image1
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchEnriched_Labeled(UpdateTrackerPersistentStore db, String batch, String roundTrip, String film,
                                      String edition, String page1, String image1) throws UpdateTrackerStorageException, FedoraFailedException {
        db.objectStateChanged(batch, new Date(), INACTIVE, 1);
        final Date roundTripLabels = new Date();
        db.objectStateChanged(roundTrip, roundTripLabels, INACTIVE, 1);
        db.objectStateChanged(film, new Date(), INACTIVE, 1);
        db.objectStateChanged(edition, new Date(), INACTIVE, 1);
        db.objectStateChanged(page1, new Date(), INACTIVE, 1);
        db.objectStateChanged(image1, new Date(), INACTIVE, 1);
        return roundTripLabels;
    }

    /**
     * The events happening when a batch is histogrammed
     * @param db
     * @param roundTrip
     * @param image1
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchHistogrammed(UpdateTrackerPersistentStore db, String roundTrip, String image1) throws
                                                                    UpdateTrackerStorageException,
                                                                    FedoraFailedException {
        db.datastreamChanged(image1, new Date(), HISTOGRAM, 1);
        final Date histograEvents = new Date();
        db.datastreamChanged(roundTrip, histograEvents, EVENTS, 1);
        return histograEvents;
    }

    /**
     * The events happening when a batch is jpylyzed
     * @param db
     * @param roundTrip
     * @param image1
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchJpylyzed(UpdateTrackerPersistentStore db, String roundTrip, String image1) throws
                                                                UpdateTrackerStorageException,
                                                                FedoraFailedException {
        db.datastreamChanged(image1, new Date(), JPYLYZER, 1);
        final Date jpylyzerEvents = new Date();
        db.datastreamChanged(roundTrip, jpylyzerEvents, EVENTS, 1);
        return jpylyzerEvents;
    }

    /**
     * The events happening when a batch is bitrepo ingested
     * @param db
     * @param roundTrip
     * @param image1
     * @return when events is updated
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchBitRepoIngested(UpdateTrackerPersistentStore db, String roundTrip, String image1) throws
                                                                       UpdateTrackerStorageException,
                                                                       FedoraFailedException {
        db.datastreamChanged(image1, new Date(), CONTENT, 1);
        db.datastreamChanged(image1, new Date(), RELS_INT, 1);
        final Date bitRepoEvents = new Date();
        db.datastreamChanged(roundTrip, bitRepoEvents, EVENTS, 1);
        return bitRepoEvents;
    }

    /**
     * The events happening when a batch is  doms ingested
     * @param db
     * @param roundTrip
     * @param film
     * @param edition
     * @param page1
     * @param image1
     * @return when events is updated
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchIngested(UpdateTrackerPersistentStore db, String roundTrip, String film, String edition,
                              String page1, String image1) throws
                                                                                                           UpdateTrackerStorageException,
                                                                                                           FedoraFailedException {
        db.objectCreated(film, new Date(), 1);
        db.datastreamChanged(film, new Date(), FILM, 1);
        db.objectCreated(edition, new Date(), 1);
        db.datastreamChanged(edition, new Date(), EDITION, 1);
        db.objectCreated(page1, new Date(), 1);
        db.datastreamChanged(page1, new Date(), MODS, 1);
        db.datastreamChanged(page1, new Date(), MIX, 1);
        db.datastreamChanged(page1, new Date(), ALTO, 1);
        db.objectCreated(image1, new Date(), 1);
        db.objectRelationsChanged(page1, new Date(), 1);
        db.objectRelationsChanged(edition, new Date(), 1);
        db.objectRelationsChanged(film, new Date(), 1);
        db.objectRelationsChanged(roundTrip, new Date(), 1);
        final Date domsIngestEvents = new Date();
        db.datastreamChanged(roundTrip, domsIngestEvents, EVENTS, 1);
        return domsIngestEvents;
    }

    /**
     * Events happening when a batch is triggered
     * @param db
     * @param batch
     * @param roundTrip
     * @return when events is updated
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date batchTriggered(UpdateTrackerPersistentStore db, String batch, String roundTrip) throws
                                                                UpdateTrackerStorageException,
                                                                FedoraFailedException {
        db.objectCreated(batch, new Date(), 1);
        db.objectCreated(roundTrip, new Date(), 1);
        db.objectRelationsChanged(batch, new Date(), 1);
        final Date triggerEvents = new Date();
        db.datastreamChanged(roundTrip, triggerEvents, EVENTS, 1);
        return triggerEvents;
    }

    /**
     * Verify no hits since epoch for the given viewAngle
     * @param db
     * @param viewAngle
     * @throws UpdateTrackerStorageException
     */
    static void verifyNoHits(UpdateTrackerPersistentStore db, String viewAngle) throws UpdateTrackerStorageException {
        List<Record> items;
        items = db.lookup(new Date(1), viewAngle, 0, 10, null, NewspaperIT.COLLECTION);
        assertEquals(0, items.size());

    }

    /**
     * Verify two hits in the list
     * @param items
     * @param hit1 pid of first hit
     * @param hit1date date for change of first hit
     * @param hit2 pid of second hit
     * @param hit2date date for change of second hit
     */
    static void verifyTwoHits(List<Record> items, String hit1, Date hit1date, String hit2, Date hit2date) {
        assertEquals(2, items.size());
        assertEquals(hit1,
                     items.get(0)
                          .getEntryPid());
        assertEquals(hit1date,
                     items.get(0)
                          .getDateForChange());

        assertEquals(hit2,
                     items.get(1)
                          .getEntryPid());
        assertEquals(hit2date,
                     items.get(1)
                          .getDateForChange());
    }

    static void verifyThreeHits(List<Record> items, String hit1, Date hit1date, String hit2, Date hit2date, String hit3,
                                Date hit3date) {
        assertEquals(3, items.size());
        assertEquals(hit1,
                     items.get(0)
                          .getEntryPid());
        assertEquals(hit1date,
                     items.get(0)
                          .getDateForChange());

        assertEquals(hit2,
                     items.get(1)
                          .getEntryPid());
        assertEquals(hit2date,
                     items.get(1)
                          .getDateForChange());

        assertEquals(hit3,
                     items.get(2)
                          .getEntryPid());
        assertEquals(hit3date,
                     items.get(2)
                          .getDateForChange());
    }


    /**
     * Verify one hit in list
     * @param items
     * @param pid
     * @param dateForChange
     */
    static void verifyOneHit(List<Record> items, String pid, Date dateForChange) {
        assertEquals(1, items.size());
        assertEquals(pid,items.get(0).getEntryPid());
        assertEquals(dateForChange, items.get(0).getDateForChange());
    }

    /**
     * Simulate a run of the edition maintainer
     * @param db
     * @param newspaper
     * @param edition
     * @param page1
     * @param fcmock
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date runEditionMaintainer(UpdateTrackerPersistentStore db, String newspaper, String edition, String page1,
                                     FedoraForUpdateTracker fcmock) throws
                                                                                      UpdateTrackerStorageException,
                                                                                      FedoraFailedException {
        //edition maintainer
        //unpublish edition
        db.objectStateChanged(edition, new Date(), INACTIVE, 1);

        //add/remove relation
        final Date editionLinkedToNewspaper = new Date();
        TestHelpers.linkEditionToNewspaper(fcmock, page1, editionLinkedToNewspaper, edition, newspaper);
        db.objectRelationsChanged(edition, editionLinkedToNewspaper, 1);

        //publish edition
        db.objectStateChanged(edition, new Date(), ACTIVE, 1);
        // add event to edition
        final Date maintainerDone = new Date();
        db.datastreamChanged(edition, maintainerDone, EDITION, 1);
        return maintainerDone;
    }

    /**
     * Publish an edition recursively
     * @param db
     * @param edition
     * @param page1
     * @param image1
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date publishEdition(UpdateTrackerPersistentStore db, String edition, String page1, String image1) throws
                                                                           UpdateTrackerStorageException,
                                                                           FedoraFailedException {
        final Date editionPublished = new Date();
        db.objectStateChanged(edition, editionPublished, ACTIVE, 1);
        db.objectStateChanged(page1, new Date(), ACTIVE, 1);
        db.objectStateChanged(image1, new Date(), ACTIVE, 1);
        return editionPublished;
    }

    /**
     * Enrich an edition and children
     * @param db
     * @param edition
     * @param page1
     * @param image1
     * @param fcmock
     * @return
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static Date enrichMimetypesAndRelations(UpdateTrackerPersistentStore db, String edition, String page1,
                                            String image1, FedoraForUpdateTracker fcmock) throws
                                                                                          UpdateTrackerStorageException,
                                                                                          FedoraFailedException {
        db.datastreamChanged(page1, new Date(), MODS, 1);
        db.datastreamChanged(page1, new Date(), MIX, 1);
        db.datastreamChanged(page1, new Date(), ALTO, 1);
        final Date pageContentModel = new Date();
        setContentModelPage(page1, pageContentModel, image1, edition, fcmock);
        db.objectRelationsChanged(page1, pageContentModel, 1);

        db.datastreamChanged(edition, new Date(), EDITION, 1);
        final Date editionContentModel = new Date();
        setContentModelEditionAndItem(edition, editionContentModel, page1, image1, fcmock);
        db.objectRelationsChanged(edition, editionContentModel, 1);
        return editionContentModel;
    }

    /**
     * Ingest an edition recursively
     * @param db
     * @param edition
     * @param page1
     * @param image1
     * @throws UpdateTrackerStorageException
     * @throws FedoraFailedException
     */
    static void ingestEdition(UpdateTrackerPersistentStore db, String edition, String page1, String image1) throws
                                                                          UpdateTrackerStorageException,
                                                                          FedoraFailedException {
        db.objectCreated(edition, new Date(), 1);
        db.datastreamChanged(edition, new Date(), EDITION, 1);
        db.objectCreated(page1, new Date(), 1);
        db.datastreamChanged(page1, new Date(), MODS, 1);
        db.datastreamChanged(page1, new Date(), MIX, 1);
        db.datastreamChanged(page1, new Date(), ALTO, 1);
        db.objectCreated(image1, new Date(), 1);
        db.objectRelationsChanged(page1, new Date(), 1);
        db.objectRelationsChanged(edition, new Date(), 1);
    }

    /**
     * Create a title record
     * @param db
     * @param newspaper
     * @param fcmock
     * @return
     * @throws FedoraFailedException
     * @throws UpdateTrackerStorageException
     */
    static Date createNewspaperObject(UpdateTrackerPersistentStore db, String newspaper, FedoraForUpdateTracker fcmock) throws FedoraFailedException, UpdateTrackerStorageException {
        final Date newspaperCreated = new Date();
        setContentModelNewspaper(newspaper,newspaperCreated, fcmock);
        db.objectCreated(newspaper, newspaperCreated, 1);
        return newspaperCreated;
    }
}
