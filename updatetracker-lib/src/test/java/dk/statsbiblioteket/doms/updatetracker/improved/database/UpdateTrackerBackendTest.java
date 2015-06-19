package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.hibernate.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State.ACTIVE;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State.DELETED;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State.INACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class UpdateTrackerBackendTest {

    protected static final String COLLECTION = "doms:Root_Collection";
    protected static final String VIEW_ANGLE = "SummaVisible";


    private FedoraForUpdateTracker fcmock;
    private DB dbSession;
    private UpdateTrackerBackend uptrack;

    @Before
    public void setUp() throws Exception {


        fcmock = mock(FedoraForUpdateTracker.class);
        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(TestHelpers.asSet(COLLECTION));
        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptySet());
        uptrack = new UpdateTrackerBackend(fcmock, 10000L, Executors.newSingleThreadExecutor());

        DBFactory dbfac = mock(DBFactory.class);
        dbSession = mock(DB.class);
        Transaction transaction = mock(Transaction.class);
        when(dbfac.createDBConnection()).thenReturn(dbSession);
        when(dbSession.beginTransaction()).thenReturn(transaction);

    }

    @Test
    public void testGetKey(){
        Date dateKey = new Date(1000);
        Record recordKey = new Record("entryPid", "viewAngle", "collection");
        String key = UpdateTrackerBackend.toKey(recordKey, dateKey);
        assertEquals("entryPid,viewAngle,collection,1000",key);
    }

    @Test
    public void testGetKeyWithExtra(){
        Date dateKey = new Date(1000);
        Record recordKey = new Record(
                "entryPid",
                "viewAngle",
                "collection",
                new Date(),
                new Date(),
                new Date(),
                new Date(),
                new HashSet<>(Arrays.asList("1","2","dsafds")));
        String key = UpdateTrackerBackend.toKey(recordKey, dateKey);
        assertEquals("entryPid,viewAngle,collection,1000",key);
    }


    @Test
    public void testGetViewBundleWithMutableKey() throws FedoraFailedException {
        //This tests tests that the viewBundleCache is not affected by any of the
        // possible modifications on Record.

        //A NEW viewbundle object is returned on each invocation.
        when(fcmock.calcViewBundle(anyString(),anyString(),any(Date.class))).thenReturn(new ViewBundle("viewBundleEntry","viewAngle"));
        UpdateTrackerBackend backend = new UpdateTrackerBackend(fcmock, 10000L, null);
        //Setup the keys
        Date dateKey = new Date();
        Record recordKey = new Record("entryPid", "viewAngle", "collection");

        //Get the first view bundle
        ViewBundle viewBundle = backend.getViewBundle(dateKey, recordKey);

        //We now change the key. The premise here is that this change should not matter
        recordKey.setActive(new Date());

        //assert that the new key will NOT get us a new viewBundle
        assertSame(viewBundle,backend.getViewBundle(dateKey, recordKey));

        //And verify that just one viewBundle was ever created
        verify(fcmock,times(1)).calcViewBundle("entryPid","viewAngle",dateKey);
        verifyNoMoreInteractions(fcmock);
    }

    @Test
    public void testModifyStateExisting() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));

        addEntry(pid);
        when(dbSession.getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION))).thenReturn(new Record(pid,
                                                                             VIEW_ANGLE,
                                                                             COLLECTION,
                                                                             null,
                                                                             new Date(10),
                                                                             null,
                                                                             null,
                                                                             TestHelpers.asSet(pid)));

        uptrack.modifyState(pid, now, COLLECTION, INACTIVE, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        //Get all records where this is the entry, as we will need to update state
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        //Get the entry angles of this record
        mocks.verify(fcmock).getEntryAngles(pid, now);
        //Check that it does not already exists, it does not
        mocks.verify(dbSession).getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION));
        //save it
        mocks.verify(dbSession).saveRecord(newRecord);  //Because getPersistentRecord give null (ie. not existing)

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testModifyState() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);

        uptrack.modifyState(pid,now,COLLECTION, INACTIVE,dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        //Get all records where this is the entry, as we will need to update state
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        //Get the entry angles of this record
        mocks.verify(fcmock).getEntryAngles(pid, now);
        //Check that it does not already exists, it does not
        mocks.verify(dbSession).getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION));
        //save it
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));
        mocks.verify(dbSession).saveRecord(newRecord);  //Because getPersistentRecord give null (ie. not existing)

        verifyNoMoreInteractions(dbSession, fcmock);
    }

    /**
     * @see #testModifyState()
     * Only change is the active timestamp is now set in the saveOrUpdate call
     * @throws Exception
     */
    @Test
    public void testModifyStateActive() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);

        uptrack.modifyState(pid, now, COLLECTION, ACTIVE, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).getPersistentRecord(new Record(pid, VIEW_ANGLE, COLLECTION));
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, now, now, null, null, TestHelpers.asSet(pid));
        mocks.verify(dbSession).saveRecord(newRecord);
        verifyNoMoreInteractions(dbSession,fcmock);
    }


    @Test
    public void testModifyStateDeleted() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));


        uptrack.modifyState(pid, now, COLLECTION, DELETED, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);

        //Find all records containing this pid; there is one
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        //Delete it
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, null, now, null, TestHelpers.emptySet(String.class));
        mocks.verify(dbSession).saveRecord(newRecord);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testModifyStateDeletedWithChildren() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid,"doms:pid2","doms:pid3");
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid,
                                                                                                                          "doms:pid2",
                                                                                                                          "doms:pid3"));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));

        uptrack.modifyState(pid, now, COLLECTION, DELETED, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        //Find all records containing this pid
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        //Set the deleted timestamp, remove the other timestamps and clear the objects list
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, null, now, null, TestHelpers
                                                                                                        .emptySet(String.class));
        mocks.verify(dbSession).saveRecord(newRecord);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    /**
     * Reconnect object called where the view bundle will contain one more object than is in the database
     * @throws Exception
     */
    @Test
    public void testReconnectObjectsOneAdded() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid, "doms:pid2");
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        //Before, only pid is in the objects set
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));

        Set<Record> changedRecords = uptrack.recalculateRecordsBasedOnThisPid(pid,
                                                                              now,
                                                                              dbSession,
                                                                              TestHelpers.asSet(COLLECTION),
                                                                              DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);

        //Find the entry angles
        mocks.verify(fcmock).getEntryAngles(pid,now);
        //Check existence; it exists
        mocks.verify(dbSession).getPersistentRecord(recordToLookup);
        //Find other entries to unlink; none
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                         .asSet(COLLECTION));
        //Find all records where this is part; only one
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        //Calc the new view bundle
        mocks.verify(fcmock).calcViewBundle(pid,VIEW_ANGLE,now);
        //Save a new version with updated timestamp and another object in the objects list
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid,
                                                                                                                  "doms:pid2"));
        Assert.assertEquals("Wrong list of changed objects",changedRecords,TestHelpers.asSet(newRecord));
        verifyNoMoreInteractions(dbSession, fcmock);
    }

    /**
     * ReconnectObjects where one object is removed from the bundle
     * @throws Exception
     */
    @Test
    public void testReconnectObjectsOneRemoved() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        //Before the bundle had 2 objects
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                   .asSet(pid,
                                                                                                                          "doms:pid2"));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsContainingThisPid(pid)).thenReturn(TestHelpers.asSet(recordBefore));

        Set<Record> changedRecords = uptrack.recalculateRecordsBasedOnThisPid(pid,
                                                                              now,
                                                                              dbSession,
                                                                              TestHelpers.asSet(COLLECTION),
                                                                              DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).getPersistentRecord(recordToLookup);
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                         .asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsContainingThisPid(pid);
        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);
        //Now the bundle have just one object
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid));
        Assert.assertEquals("Wrong list of changed objects", changedRecords, TestHelpers.asSet(newRecord));
        verifyNoMoreInteractions(dbSession, fcmock);
    }

    /**
     * Two records, sharing a child. Record 1 is updated. Record 2 should not be affected
     * @throws Exception
     */
    @Test
    public void testReconnectObjectsChildInTwoRecords() throws Exception {
        String pid1 = "doms:entry1";
        String pid2 = "doms:entry2";
        String child1 = "doms:child1";
        Date now = new Date();

        addEntry(pid1,child1);
        addEntry(pid2,child1);
        final Record recordToLookup = new Record(pid1, VIEW_ANGLE, COLLECTION);
        //Record 1 does not know it contains child1
        final Record record1Before = new Record(pid1, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, TestHelpers
                                                                                                                     .asSet(pid1));
        //Record 2 does know it
        final Record record2Before = new Record(pid2,
                                               VIEW_ANGLE,
                                               COLLECTION,
                                               null,
                                               new Date(1),
                                               null,
                                               null,
                                               TestHelpers.asSet(pid2, child1));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(record1Before);
        when(dbSession.getRecordsContainingThisPid(child1)).thenReturn(TestHelpers.asSet(record1Before, record2Before));

        Set<Record> changedRecords = uptrack.recalculateRecordsBasedOnThisPid(pid1,
                                                                              now,
                                                                              dbSession,
                                                                              TestHelpers.asSet(COLLECTION),
                                                                              DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        //Get the entry angles, in this case just one
        mocks.verify(fcmock).getEntryAngles(pid1, now);
        //So, there could be an object already, check for it
        mocks.verify(dbSession).getPersistentRecord(recordToLookup);
        //Check if there is other records in collections or view angles no longer used by this obejct
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid1, TestHelpers.asSet(VIEW_ANGLE), TestHelpers
                                                                                                                          .asSet(COLLECTION));
        //Find the records which this object is part; just one
        mocks.verify(dbSession).getRecordsContainingThisPid(pid1);
        //calc the view bundle for each
        mocks.verify(fcmock).calcViewBundle(pid1, VIEW_ANGLE, now);
        //If changed, save
        final Record newRecord = new Record(pid1, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid1,
                                                                                                                   child1));
        Assert.assertEquals("Wrong list of changed objects", changedRecords, TestHelpers.asSet(newRecord));

        //Notice that record 2 is not affected at all
        verifyNoMoreInteractions(dbSession, fcmock);
    }


    /**
     * * Two records, sharing a child. The child is updated. Record 1 should be updated. Record 2 should have its viewbundle recalculated
     * @throws Exception
     */
    @Test
    public void testReconnectChildInTwoRecords() throws Exception {
        String pid1 = "doms:entry1";
        String pid2 = "doms:entry2";
        String child1 = "doms:child1";
        Date now = new Date();

        addEntry(pid1, child1);
        addEntry(pid2, child1);
        final Record recordToLookup = new Record(child1, VIEW_ANGLE, COLLECTION);
        final Record record1Before = new Record(pid1,
                                                VIEW_ANGLE,
                                                COLLECTION,
                                                null,
                                                new Date(1),
                                                null,
                                                null,
                                                TestHelpers.asSet(pid1));
        final Record record2Before = new Record(pid2,
                                                VIEW_ANGLE,
                                                COLLECTION,
                                                null,
                                                new Date(1),
                                                null,
                                                null,
                                                TestHelpers.asSet(pid2, child1));

        when(dbSession.getPersistentRecord(eq(recordToLookup)))
                .thenReturn(null);
        when(dbSession.getRecordsContainingThisPid(child1)).thenReturn(TestHelpers.asSet(record1Before, record2Before));

        Set<Record> changedRecords = uptrack.recalculateRecordsBasedOnThisPid(child1,
                                                                              now,
                                                                              dbSession,
                                                                              TestHelpers.asSet(COLLECTION),
                                                                              DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        //Check for entry angles, there are none
        mocks.verify(fcmock).getEntryAngles(child1, now);
        //Find other records, there are none
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(child1,
                                                                             TestHelpers.emptySet(String.class),
                                                                             TestHelpers.asSet(COLLECTION));
        //Find records with this object
        mocks.verify(dbSession).getRecordsContainingThisPid(child1);
        //For each calc the view bundle
        mocks.verify(fcmock).calcViewBundle(pid2, VIEW_ANGLE, now);
        //Pid2 bundle not changed so no save here

        //Calc pid 1 bundle
        mocks.verify(fcmock).calcViewBundle(pid1, VIEW_ANGLE, now);

        //pid 1 bundle changed, so save
        final Record newRecord = new Record(pid1, VIEW_ANGLE, COLLECTION, null, now, null, null, TestHelpers.asSet(pid1,
                                                                                                                   child1));
        Assert.assertEquals("Wrong list of changed objects", changedRecords, TestHelpers.asSet(newRecord));
        verifyNoMoreInteractions(dbSession, fcmock);
    }


    private void addEntry(String pid, String... contained) throws FedoraFailedException {
        TestHelpers.addEntry(pid,fcmock,contained);
    }
}