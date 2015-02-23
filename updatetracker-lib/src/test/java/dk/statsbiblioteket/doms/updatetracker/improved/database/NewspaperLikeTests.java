package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record.State;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.HibernateUtils.set;
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

public class NewspaperLikeTests {

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
    UpdateTrackerPersistentStore db;
    Fedora fcmock;

    public NewspaperLikeTests() throws MalformedURLException {
        fcmock = mock(Fedora.class);
        //fcmock = new FedoraMockup(new Credentials("user", "pass"), null, null);
    }


    @Before
    public void setUp() throws Exception {
        db = new UpdateTrackerPersistentStoreImpl(fcmock);
        db.setUp();
    }

    @After
    public void tearDown() throws Exception {
        db.clear();
        db.close();
    }

    @Test
    public void testEventsChanged() throws UpdateTrackerStorageException, FedoraFailedException {

        Date beginning = new Date(1);
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(set(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        when(fcmock.getViewInfo(eq(roundTrip), any(Date.class))).thenReturn(asList(new ViewInfo(viewAngle,
                                                                                                       true,
                                                                                                       roundTrip)));
        db.objectCreated(roundTrip, beginning);

        List<Record> list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(list.get(0).getDateForChange().getTime(), beginning.getTime());

        Date eventAdded = new Date();
        db.datastreamChanged(roundTrip, eventAdded, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(eventAdded.getTime(), list.get(0).getDateForChange().getTime());

        Date eventAdded2 = new Date();
        db.datastreamChanged(roundTrip, eventAdded2, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(eventAdded2.getTime(), list.get(0).getDateForChange().getTime());


        Date eventAdded3 = new Date();
        db.datastreamChanged(roundTrip, eventAdded3, EVENTS);

        list = db.lookup(beginning, viewAngle, 0, 100, null, collection);
        assertEquals(1, list.size());
        assertEquals(eventAdded3.getTime(), list.get(0).getDateForChange().getTime());
    }


    @Test
    public void testBecomingItem() throws FedoraFailedException, UpdateTrackerStorageException {

        Date beginning = new Date(0);
        Date becomingItemTime = new Date();
        Date eventAdded = new Date();
        final String roundTrip = "doms:roundTrip1";
        final String collection = "doms:Root_Collection";
        String viewAngle = SBOI;

        when(fcmock.getCollections(eq(roundTrip), any(Date.class))).thenReturn(set(collection));
        when(fcmock.calcViewBundle(eq(roundTrip), eq(viewAngle), any(Date.class))).thenReturn(new ViewBundle(roundTrip,
                                                                                                                    viewAngle));
        //Not a entry object before this time
        when(fcmock.getViewInfo(eq(roundTrip), lt(becomingItemTime))).thenReturn(asList());

        //But after this time, the object is an entry
        when(fcmock.getViewInfo(eq(roundTrip), geq(becomingItemTime))).thenReturn(asList(new ViewInfo(viewAngle,
                                                                                                             true,
                                                                                                             roundTrip)));


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
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(set(collection));

        //No entry objects or view stuff until initialised
        when(fcmock.getViewInfo(anyString(), any(Date.class))).thenReturn(asList());

        //Content Model for roundtrip
        setContentModelItem(roundTrip);

        //Not a entry object before this time

        List<Record> items;
        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(0, items.size());


        //Triggered
        db.objectCreated(batch, new Date());
        db.objectCreated(roundTrip, new Date());
        db.objectRelationsChanged(batch, new Date());
        final Date triggerEvents = new Date();
        db.datastreamChanged(roundTrip, triggerEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(triggerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //This simulates the order in which objects are created by the doms ingester
        db.objectCreated(film, new Date());
        db.datastreamChanged(film, new Date(), FILM);
        db.objectCreated(edition, new Date());
        db.datastreamChanged(edition, new Date(), EDITION);
        db.objectCreated(page1, new Date());
        db.datastreamChanged(page1, new Date(), MODS);
        db.datastreamChanged(page1, new Date(), MIX);
        db.datastreamChanged(page1, new Date(), ALTO);
        db.objectCreated(image1, new Date());
        db.objectRelationsChanged(page1, new Date());
        db.objectRelationsChanged(edition, new Date());
        db.objectRelationsChanged(film, new Date());
        db.objectRelationsChanged(roundTrip, new Date());
        final Date domsIngestEvents = new Date();
        db.datastreamChanged(roundTrip, domsIngestEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(domsIngestEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        // And then the bit repo ingester
        db.datastreamChanged(image1, new Date(), CONTENT);
        db.datastreamChanged(image1, new Date(), RELS_INT);
        final Date bitRepoEvents = new Date();
        db.datastreamChanged(roundTrip, bitRepoEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(bitRepoEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());

        //Jpylyzer
        db.datastreamChanged(image1, new Date(), JPYLYZER);
        final Date jpylyzerEvents = new Date();
        db.datastreamChanged(roundTrip, jpylyzerEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(jpylyzerEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //Histogram
        db.datastreamChanged(image1, new Date(), HISTOGRAM);
        final Date histograEvents = new Date();
        db.datastreamChanged(roundTrip, histograEvents, EVENTS);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(histograEvents.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());


        //Then we come to the enricher
        //Labels
        db.objectStateChanged(batch, new Date(), INACTIVE);
        final Date roundTripLabels = new Date();
        db.objectStateChanged(roundTrip, roundTripLabels, INACTIVE);
        db.objectStateChanged(film, new Date(), INACTIVE);
        db.objectStateChanged(edition, new Date(), INACTIVE);
        db.objectStateChanged(page1, new Date(), INACTIVE);
        db.objectStateChanged(image1, new Date(), INACTIVE);

        items = db.lookup(beginning, SBOI, 0, 10, null, collection);
        assertEquals(1, items.size());
        assertEquals(roundTripLabels.getTime(), items.get(0).getDateForChange().getTime());
        assertEquals(State.INACTIVE, items.get(0).getState());

        //mimetype and relations and content models
        db.datastreamChanged(page1, new Date(), MODS);
        db.datastreamChanged(page1, new Date(), MIX);
        db.datastreamChanged(page1, new Date(), ALTO);
        final Date pageContentModel = new Date();
        hasContentModelPage(page1, pageContentModel, image1, edition);
        db.objectRelationsChanged(page1, pageContentModel);

        db.datastreamChanged(edition, new Date(), EDITION);
        final Date editionContentModel = new Date();
        hasContentModelEditionAndItem(edition, editionContentModel,page1, image1);
        db.objectRelationsChanged(edition, editionContentModel);

        db.datastreamChanged(film, new Date(), FILM);
        final Date filmContentModel = new Date();
        hasContentModelFilm(film, filmContentModel, edition, page1, image1);
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
        db.objectStateChanged(roundTrip, new Date(), ACTIVE);
        db.objectStateChanged(film, new Date(), ACTIVE);
        db.objectStateChanged(edition, new Date(), ACTIVE);
        db.objectStateChanged(page1, new Date(), ACTIVE);
        db.objectStateChanged(image1, new Date(), ACTIVE);

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

    private void setContentModelItem(String roundTrip) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(roundTrip), eq(SBOI), any(Date.class))).thenReturn(new ViewBundle(roundTrip, SBOI));
        when(fcmock.getViewInfo(eq(roundTrip), gt(new Date()))).thenReturn(asList(new ViewInfo(SBOI, true, roundTrip)));
    }

    private void hasContentModelFilm(String pid, Date date, String edition, String page, String image) throws FedoraFailedException {
        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(date))).thenReturn(new ViewBundle(pid,
                                                                                                  GUI,
                                                                                                    asList(pid, edition, page, image)));
    }

    private void hasContentModelEditionAndItem(String pid, Date date, String page, String image) throws FedoraFailedException {

        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(date))).thenReturn(new ViewBundle(pid,
                                                                                                  GUI,
                                                                                                    asList(pid, page, image)));
        //TODO no current handling of Newspaper Objects
        when(fcmock.calcViewBundle(eq(pid), eq(SUMMA_VISIBLE), geq(date))).thenReturn(new ViewBundle(pid, SUMMA_VISIBLE,
                                                                                                             asList(pid)));
        when(fcmock.calcViewBundle(eq(pid), eq(SBOI), geq(date))).thenReturn(new ViewBundle(pid, SBOI,
                                                                                                     asList(pid)));


        when(fcmock.getViewInfo(eq(pid), geq(date))).thenReturn(asList(new ViewInfo(GUI, true, pid),
                                                                              new ViewInfo(SBOI, true, pid)));
    }

    private void hasContentModelPage(String pid, Date pageContentModel, String image, String edition) throws
                                                                                                      FedoraFailedException {
        when(fcmock.calcViewBundle(eq(pid), eq(GUI), geq(pageContentModel))).thenReturn(new ViewBundle(pid, GUI,
                                                                                                                asList(pid,
                                                                                                                              image)));
        when(fcmock.calcViewBundle(eq(pid), eq(SUMMA_VISIBLE), geq(pageContentModel))).thenReturn(new ViewBundle(pid,
                                                                                                                        SUMMA_VISIBLE,
                                                                                                                         asList(pid,
                                                                                                                                       edition)));

        //But after this time, the object is an entry
        when(fcmock.getViewInfo(eq(pid), geq(pageContentModel))).thenReturn(asList(new ViewInfo(SUMMA_VISIBLE,
                                                                                                       true,
                                                                                                       pid)));
    }

    public void testUpdateToNewspaper() {


    }

    public void testUpdateToEdition() {


    }
}
