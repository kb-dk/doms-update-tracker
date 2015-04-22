package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.StatelessDB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UpdateTrackerPersistentStoreTest {

    protected static final String COLLECTION = "doms:Root_Collection";
    private final String collection = COLLECTION;
    UpdateTrackerPersistentStore store;
    FedoraForUpdateTracker fcmock;
    private DB dbSession;
    protected static final String VIEW_ANGLE = "SummaVisible";

    @Before
    public void setUp() throws Exception {
        File configFile = new File(Thread.currentThread()
                                         .getContextClassLoader()
                                         .getResource("hibernate.cfg.xml")
                                         .toURI());
        File mappings = new File(Thread.currentThread().getContextClassLoader().getResource("updateTrapperMappings.xml")
                                       .toURI());


        fcmock = mock(FedoraForUpdateTracker.class);
        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(asSet(collection));
        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptySet());
        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fcmock,10000L);

        DBFactory dbfac = mock(DBFactory.class);
        dbSession = mock(DB.class);
        Transaction transaction = mock(Transaction.class);
        when(dbfac.getInstance()).thenReturn(dbSession);
        when(dbSession.beginTransaction()).thenReturn(transaction);
        StatelessDB statelessDB = mock(StatelessDB.class);
        when(dbfac.getStatelessDB()).thenReturn(statelessDB);

        store = new UpdateTrackerPersistentStoreImpl(fcmock, updateTrackerBackend, dbfac);
    }


    static <T> Set<T> asSet(T... vars) {
        return new HashSet<T>(Arrays.asList(vars));
    }


    @After
    public void tearDown() throws Exception {
        store.close();
    }

    @Test
    public void testObjectCreatedBasic() throws Exception {
        init();
        Date now = new Date();
        final String pid = "doms:test1";
        addEntry(pid);
        final int key = 1;
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid));
        when(dbSession.recordExists(eq(new Record(pid, VIEW_ANGLE, COLLECTION))))
                .thenReturn(null)
                .thenReturn(newRecord);
        store.objectCreated(pid, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);

        //ObjectCreated
        mocks.verify(dbSession).beginTransaction();
        mocks.verify(fcmock).getCollections(pid, now);
        mocks.verify(fcmock).getState(pid, now);

        //ModifyState
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(new Record(pid, VIEW_ANGLE, COLLECTION));
        mocks.verify(dbSession).saveOrUpdate(newRecord);

        //Reconnect Objects
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(new Record(pid, VIEW_ANGLE, COLLECTION));
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(pid);
        mocks.verify(fcmock).calcViewBundle(pid,VIEW_ANGLE,now);

        //Update Dates
        mocks.verify(dbSession).updateDates(pid,now);

        //Latest key
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession,fcmock);

    }

    private void init() throws Exception {
        //tearDown();
        //setUp();
    }

    @Test
    public void testIdempotency() throws Exception {

        //This test tries to ingest and publish and delete an object. Then it does it again, with the same timestamps
        //and the same asserts, to see if the system ends up in the same state
        init();
        Date test1Create = new Date(1);
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1Create, 1);

        List<Record> list;

        final Date test1Published = new Date();
        store.objectStateChanged("doms:test1", test1Published, "A", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals("Object not Active at the right timestamp",test1Published.getTime(), list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals("Object not changed at published timestamp",
                     test1Published.getTime(),
                     list.get(0).getInactive().getTime());

        final Date test1Deleted = new Date();
        store.objectStateChanged("doms:test1", test1Deleted, "D", 1);

        //After delete, the object is no longer published
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", 1, list.size());
        assertEquals("Object not deleted",list.get(0).getState(), Record.State.DELETED);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals("Object not marked as deleted at the right time",test1Deleted.getTime(), list.get(0).getDeleted().getTime());

        //This was the first pass, now do it again

        store.objectCreated("doms:test1", test1Create, 1);
        store.objectStateChanged("doms:test1", test1Published, "A", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals("Object not Active at the right timestamp",
                     test1Published.getTime(),
                     list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals("Object not changed at published timestamp",
                     test1Published.getTime(),
                     list.get(0).getInactive().getTime());

        store.objectStateChanged("doms:test1", test1Deleted, "D", 1);

        //After delete, the object is no longer published
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", 1, list.size());
        assertEquals("Object not deleted", list.get(0).getState(), Record.State.DELETED);

        list = store.lookup(test1Create, VIEW_ANGLE, 0, 100, null, COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals("Object not marked as deleted at the right time",
                     test1Deleted.getTime(),
                     list.get(0).getDeleted().getTime());
    }

    private void addEntry(String pid, String... contained) throws FedoraFailedException {
        when(fcmock.getEntryAngles(eq(pid), any(Date.class))).thenReturn(asSet(VIEW_ANGLE));
        when(fcmock.getState(eq(pid), any(Date.class))).thenReturn(Record.State.INACTIVE);
        List < String > objects = new ArrayList<String>(Arrays.asList(contained));
        objects.add(pid);
        when(fcmock.calcViewBundle(eq(pid),eq(VIEW_ANGLE),any(Date.class))).thenReturn(new ViewBundle(pid,
                                                                                                      VIEW_ANGLE,
                                                                                                     objects));
    }

    private void removeEntry(String pid) throws FedoraFailedException {

        when(fcmock.getEntryAngles(eq(pid), any(Date.class))).thenThrow(new FedoraFailedException("Object not found"));
        when(fcmock.calcViewBundle(eq(pid), eq(VIEW_ANGLE), any(Date.class))).thenThrow(
                                                                                              new FedoraFailedException("Object not found"));
    }

    @Test
    public void testObjectDeletedBasic() throws Exception {

        Date now = new Date();
        final String pid = "doms:test1";
        int key = 1;

        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid));

        when(dbSession.recordExists(eq(new Record(pid, VIEW_ANGLE, COLLECTION))))
                .thenReturn(newRecord);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(newRecord));
        store.objectDeleted(pid, now, ++key);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(dbSession).beginTransaction();
        mocks.verify(dbSession).getRecordsForPid(pid);
        mocks.verify(dbSession).saveOrUpdate(newRecord);
        mocks.verify(dbSession).updateDates(pid, now);
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testObjectRessurection() throws Exception {
        init();
        Date test1Create = new Date();
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1Create, 1);
        assertEquals("To many objects", 1, store.lookup(test1Create, "SummaVisible", 0, 100, null, COLLECTION)
                                             .size());

        Date test1Delete = new Date();
        store.objectDeleted("doms:test1", test1Delete, 1);

        //Test how many Inactive objects there are
        final List<Record> inactive = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("To many objects", 0, filter(inactive).size());
        final List<Record> deleted = store.lookup(test1Delete, "SummaVisible", 0, 100, "D", COLLECTION);
        assertEquals("To many objects", 1, deleted.size());
        assertEquals(test1Delete.getTime(), deleted.get(0).getDeleted().getTime());
        assertEquals("doms:test1", deleted.get(0).getEntryPid());

        Date test1CreateAgain = new Date();
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1CreateAgain, 1);
        assertEquals("To many objects", 1, store.lookup(test1Create, "SummaVisible", 0, 100, null,
                                                        COLLECTION).size());
        assertEquals("To many objects", 1, store.lookup(test1CreateAgain, "SummaVisible", 0, 100, null,
                                                        COLLECTION).size());
        assertEquals("To many objects", 1, store.lookup(test1Create, "SummaVisible", 0, 100, "I",
                                                        COLLECTION).size());
    }

    private Collection<Record> filter(List<Record> inactive) {
        Collection<Record> result = new ArrayList<Record>(inactive);
        CollectionUtils.filter(result, new Predicate<Record>() {
            @Override
            public boolean evaluate(Record record) {
                return record.getState() != Record.State.DELETED;
            }
        });
        return result;
    }

    @Test
    public void testObjectPublished() throws Exception {
        init();
        Date test1Create = new Date(1);
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1Create, 1);

        List<Record> list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        store.objectStateChanged("doms:test1", test1Published, "A", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

    }


    @Test
    public void testObjectPublishedAndUnpublished() throws Exception {
        init();
        Date test1Create = new Date(1);
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1Create, 1);

        List<Record> list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        store.objectStateChanged("doms:test1", test1Published, "A", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

        final Date test1Unpublished = new Date();
        store.objectStateChanged("doms:test1", test1Unpublished, "I", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Unpublished.getTime(), list.get(0).getInactive().getTime());
    }


    @Test
    public void testObjectPublishedAndDeleted() throws Exception {
        init();
        Date test1Create = new Date(1);
        addEntry("doms:test1");
        store.objectCreated("doms:test1", test1Create, 1);

        List<Record> list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        store.objectStateChanged("doms:test1", test1Published, "A", 1);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

        final Date test1Deleted = new Date();
        store.objectStateChanged("doms:test1", test1Deleted, "D", 1);

        //After delete, the object is no longer published
        list = store.lookup(test1Create, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals("Wrong number of objects", 1, list.size());
        assertEquals(list.get(0).getState(), Record.State.DELETED);

        list = store.lookup(test1Create, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Deleted.getTime(), list.get(0).getDeleted().getTime());
    }


    @Test
    public void testObjectRelationsChangedBasic() throws Exception {
        init();
        Date frozen = new Date(1);
        addEntry("doms:test1");
        store.objectCreated("doms:test1", frozen, 1);
        store.objectCreated("doms:test2", frozen, 1);
        addEntry("doms:test1", "doms:test2");

        List<Record> list = store.lookup(frozen, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(list.get(0).getInactive().getTime(), frozen.getTime());

        Date flow = new Date();
        store.objectRelationsChanged("doms:test1", flow, 1);

        list = store.lookup(frozen, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(flow.getTime(), list.get(0).getInactive().getTime());

        Thread.sleep(1000);
        Date flow2 = new Date();

        store.datastreamChanged("doms:test2", flow2, "Something", 1);

        list = store.lookup(frozen, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(flow2.getTime(), list.get(0).getInactive().getTime());

        Date flow3 = new Date();

        store.objectStateChanged("doms:test1", flow3, "A", 1);
        list = store.lookup(frozen, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(list.get(0).getInactive().getTime(), flow3.getTime());
        assertEquals(list.get(0).getActive().getTime(), flow3.getTime());
    }


    @Test
    public void testObjectRelationsChangedDeep() throws Exception {
        init();
        Date ingest1 = new Date(1);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        store.objectCreated("doms:test1", ingest1, 1);
        store.objectCreated("doms:test2", ingest2, 1);
        store.objectCreated("doms:test3", ingest3, 1);
        addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        store.objectRelationsChanged("doms:test1", test1RelChange, 1);

        list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getInactive().getTime());

        Date test3RelChange = new Date();

        store.datastreamChanged("doms:test3", test3RelChange, "DSID", 1);

        list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test3RelChange.getTime(), list.get(0).getInactive().getTime());


    }

    @Test
    public void testObjectRelationsChangedPublished() throws Exception {
        init();
        Date ingest1 = new Date(1);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        store.objectCreated("doms:test1", ingest1, 1);
        store.objectCreated("doms:test2", ingest2, 1);
        store.objectCreated("doms:test3", ingest3, 1);
        addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        store.objectRelationsChanged("doms:test1", test1RelChange, 1);

        list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getInactive().getTime());

        Date test1Publish = new Date();
        store.objectStateChanged("doms:test1", test1Publish, "A", 1);
        list = store.lookup(ingest1, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test1Publish.getTime(), list.get(0).getActive().getTime());

        Date test2Publish = new Date();
        store.objectStateChanged("doms:test2", test2Publish, "A", 1);
        list = store.lookup(ingest1, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test2Publish.getTime(), list.get(0).getActive().getTime());

        //As long as the entry is published, the state of the minors do not matter
        Date test2unPublish = new Date();
        store.objectStateChanged("doms:test2", test2unPublish, "I", 1);
        list = store.lookup(ingest1, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test2unPublish.getTime(), list.get(0).getInactive().getTime());

        Date test1unPublish = new Date();
        store.objectStateChanged("doms:test1", test1unPublish, "I", 1);
        list = store.lookup(ingest1, "SummaVisible", 0, 100, "A", COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test1unPublish.getTime(), list.get(0).getInactive().getTime());
        assertEquals(test2unPublish.getTime(), list.get(0).getActive().getTime());
        list = store.lookup(ingest1, "SummaVisible", 0, 100, "I", COLLECTION);
        assertEquals(1, list.size());
        assertEquals(test1unPublish.getTime(), list.get(0).getInactive().getTime());
    }


    @Test
    public void testObjectRelationsChangedDeepMultiple() throws Exception {
        init();
        Date ingest1 = new Date(1);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        store.objectCreated("doms:test1", ingest1, 1);
        store.objectCreated("doms:test2", ingest2, 1);
        store.objectCreated("doms:test3", ingest3, 1);
        addEntry("doms:test4", "doms:test5", "doms:test6");
        store.objectCreated("doms:test4", ingest1, 1);
        store.objectCreated("doms:test5", ingest2, 1);
        store.objectCreated("doms:test6", ingest3, 1);

        addEntry("doms:test1", "doms:test2", "doms:test3");


        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(1, list.size());

        Date test1RelChange = new Date();
        store.objectRelationsChanged("doms:test1", test1RelChange, 1);

        list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(2, list.size());
        assertEquals(test1RelChange.getTime(), list.get(1).getInactive().getTime());

        Date test3DatastreamChange = new Date();

        store.datastreamChanged("doms:test3", test3DatastreamChange, "DSID", 1);

        list = store.lookup(ingest1, "SummaVisible", 0, 100, null, COLLECTION);
        assertEquals(2, list.size());
        assertEquals(test3DatastreamChange.getTime(), list.get(1).getInactive().getTime());
    }

    @Test
    public void testLatestKeyInserted() throws Exception {
        init();
        long latestKey = store.getLatestKey();
        long newLatestKey = latestKey + 42;
        store.objectCreated("doms:test1", new Date(0L), newLatestKey);
        assertEquals("Should have updated the latest key", newLatestKey, store.getLatestKey());
    }
}
