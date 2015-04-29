package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * These tests mock up doms, but use the dockerized postgres to test the update tracker with a real database
 */
public class UpdateTrackerPersistentStoreTest {

    //TODO make tests that use a mocked UpdateTrackerBackend

    protected static final String COLLECTION = "doms:Root_Collection";
    UpdateTrackerPersistentStore store;
    FedoraForUpdateTracker fcmock;
    private DB dbSession;
    protected static final String VIEW_ANGLE = "SummaVisible";

    @Before
    public void setUp() throws Exception {
        fcmock = mock(FedoraForUpdateTracker.class);
        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(TestHelpers.asSet(COLLECTION));
        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptySet());
        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fcmock,10000L);

        DBFactory dbfac = mock(DBFactory.class);
        dbSession = mock(DB.class);
        Transaction transaction = mock(Transaction.class);
        when(dbfac.createDBConnection()).thenReturn(dbSession);
        when(dbfac.createReadonlyDBConnection()).thenReturn(dbSession);
        when(dbSession.beginTransaction()).thenReturn(transaction);

        store = new UpdateTrackerPersistentStoreImpl(fcmock, updateTrackerBackend, dbfac);
        when(fedora.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptyList());
        final ExecutorService threadPool1 = Executors.newSingleThreadExecutor();
        final ExecutorService threadPool2 = Executors.newSingleThreadExecutor();
        final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fedora,10000L,
                                                                                   threadPool1);
        db = new UpdateTrackerPersistentStoreImpl(configFile, mappings, fedora, updateTrackerBackend,
                                                  threadPool2);
    }


    @After
    public void tearDown() throws Exception {
        db.close();
    }

    @Test
    public void testContentModelRebuild() throws Exception {
        init();
        Date start = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", new Date(), 1);

        addEntry("doms:test2");
        db.objectCreated("doms:test2", new Date(), 2);

        List<Record> list = db.lookup(new Date(1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        final String cmPid = "doms:cm1";
        addEntry("doms:test1","doms:child1");
        addEntry("doms:test2","doms:child2");
        final Date cmUpdate = new Date();
        when(fedora.isCurrentlyContentModel(cmPid,cmUpdate)).thenReturn(true);
        when(fedora.getObjectsOfThisContentModel(cmPid)).thenReturn(asSet("doms:test1", "doms:test2"));
        db.objectRelationsChanged(cmPid, cmUpdate, 3);
        list = db.lookup(new Date(1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(list.get(0).getInactive(),list.get(1).getInactive());
        assertEquals(list.get(0).getInactive().getTime(), cmUpdate.getTime());
    }

    /**
     * Simulates the process when we receive a ingest event from the worklog. As can be seen from the code, we run
     * modifyState, reconnectObjects and updateDates
     * @throws Exception
     */
    @Test
    public void testObjectCreatedBasic() throws Exception {
        Date now = new Date();
        final String pid = "doms:test1";
        TestHelpers.addEntry(pid, fcmock);
        final int key = 1;
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));
        when(dbSession.getPersistentRecord(eq(new Record(pid, VIEW_ANGLE, COLLECTION))))
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
        mocks.verify(dbSession).getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION));
        mocks.verify(dbSession).saveRecord(newRecord);  //Because RecordExists give null in the first call

        //Reconnect Objects
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION)); //This does not give null, so no saveAndUpdate
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                         .asSet(COLLECTION));//Will be empty set
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);  //Should return empty due to not flushed yet
        mocks.verify(fcmock).calcViewBundle(pid,VIEW_ANGLE,now);
        //No saveAndUpdate as the contained objects have not changed

        //Update Dates
        mocks.verify(dbSession).updateDates(pid,now);

        //Latest key
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession,fcmock);

    }


    /**
     * Simulates the process when we receive an object purged or object changed state to deleted from the worklog
     * @throws Exception
     */
    @Test
    public void testObjectDeletedBasic() throws Exception {
        Date now = new Date();
        final String pid = "doms:test1";
        int key = 1;

        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));

        when(dbSession.getPersistentRecord(eq(new Record(pid, VIEW_ANGLE, COLLECTION))))
                .thenReturn(newRecord);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(newRecord));
        store.objectDeleted(pid, now, ++key);

        InOrder mocks = inOrder(dbSession, fcmock);
        //ObjectDeleted
        mocks.verify(dbSession).beginTransaction();
        //ModifyStates
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        mocks.verify(dbSession).saveRecord(newRecord);
        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }

    /**
     * Simulates the process when we receive a object changed state to active from the worklog
     * @throws Exception
     */
    @Test
    public void testObjectPublished() throws Exception {
        Date now = new Date();
        final String pid = "doms:test1";
        int key = 1;

        final Record keyedRecord = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));
        TestHelpers.addEntry("doms:test1", fcmock);


        when(dbSession.getPersistentRecord(eq(keyedRecord)))
                .thenReturn(newRecord);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(newRecord));
        store.objectStateChanged("doms:test1", now, "A", 1);

        InOrder mocks = inOrder(dbSession, fcmock);
        //ObjectStateChanged
        mocks.verify(dbSession).beginTransaction();
        mocks.verify(fcmock).getCollections(pid, now);

        //ModifyStates
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid,now);
        mocks.verify(dbSession).getPersistentRecord(keyedRecord);
        mocks.verify(dbSession).saveRecord(newRecord);
        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }

    /**
     * Simulates the process when an object's relations change, but this does not change the view bundle, ie. when
     * a non-view relation change
     * @throws Exception
     */
    @Test
    public void testObjectRelationsChangedSameBundle() throws Exception {
        final String pid = "doms:test1";
        final int key = 1;

        final String child = "doms:test2";
        TestHelpers.addEntry(pid, fcmock, child);
        Date now = new Date();
        final Record keyedRecord = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid,
                                                                                                                  child));
        when(dbSession.getPersistentRecord(eq(keyedRecord)))
                .thenReturn(newRecord);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(newRecord));

        store.objectRelationsChanged(pid, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);

        mocks.verify(dbSession).beginTransaction();

        mocks.verify(fcmock).isCurrentlyContentModel(pid, now);
        mocks.verify(fcmock).getCollections(pid, now);
        mocks.verify(fcmock).getState(pid,now);

        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).getPersistentRecord(keyedRecord);

        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                         .asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);

        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);

        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    /**
     * Simulates the process when an object's relations change, so that an additional object becomes part of the view bundle
     * @throws Exception
     */
    @Test
    public void testObjectRelationsChangedChangedBundle() throws Exception {
        final String pid = "doms:test1";
        final String child = "doms:test2";
        final int key = 1;

        TestHelpers.addEntry(pid, fcmock, child);

        Date now = new Date();
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);

        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid));

        final Record recordToSave = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers
                                                                                                           .asSet(pid,
                                                                                                                  child));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));


        store.objectRelationsChanged(pid, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);


        mocks.verify(dbSession).beginTransaction();

        //Datastream RELSEXT changed
        mocks.verify(fcmock).isCurrentlyContentModel(pid, now);
        mocks.verify(fcmock).getCollections(pid, now);
        mocks.verify(fcmock).getState(pid,now);

        //backend.reconnectObjects
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).getPersistentRecord(recordToLookup); //This returns true, so no save yet

        //Check for any old records which should be unlinked. There are none
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                         .asSet(COLLECTION));
        //Find all the records affected by this change
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        //Calc the new viewbundle for each of these
        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);
        //Save the change
        mocks.verify(dbSession).saveRecord(recordToSave);


        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    /**
     * Simulates the process when an object's relation change (the child object). The child object is not an entry object
     * The change connects it to another object, which have this relation as an inverse view relation.
     * While this does put the child in a record, this is not reflected in the database, as we do not check what the
     * relation points to.
     * @throws Exception
     */
    @Test
    public void testObjectRelationsChangedChildChanged() throws Exception {
        final String pid = "doms:test1";
        final String child = "doms:test2";
        final int key = 1;

        TestHelpers.addEntry(pid, fcmock, child);

        Date now = new Date();
        final Record recordToLookup = new Record(child, VIEW_ANGLE, COLLECTION);

        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid));


        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));
        when(dbSession.getRecordsContainingThisPid(child)).thenReturn(TestHelpers.emptySet(Record.class));

        store.objectRelationsChanged(child, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);

        mocks.verify(dbSession).beginTransaction();

        //Datastream RELSEXT changed
        mocks.verify(fcmock).isCurrentlyContentModel(child, now);
        mocks.verify(fcmock).getCollections(child, now);
        mocks.verify(fcmock).getState(child, now);


        //backend.reconnectObjects
        mocks.verify(fcmock).getEntryAngles(child, now);

        //Check for any old records which should be unlinked. There are none
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(child, TestHelpers.emptySet(String.class), TestHelpers
                                                                                                                             .asSet(COLLECTION));
        //Find all the records affected by this change
        mocks.verify(dbSession).getRecordsContainingThisPid(child);

        //UpdateDAtes
        mocks.verify(dbSession).updateDates(child, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testLatestKeyInserted() throws Exception {
        long latestKey = store.getLatestKey();
        long newLatestKey = latestKey + 42;
        store.objectCreated("doms:test1", new Date(0L), newLatestKey);
        verify(dbSession).setLatestKey(newLatestKey);
    }
}
