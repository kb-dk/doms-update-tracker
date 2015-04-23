package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.Utils;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.Utils.asSet;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UpdateTrackerPersistentStoreTest {

    protected static final String COLLECTION = "doms:Root_Collection";
    UpdateTrackerPersistentStore store;
    FedoraForUpdateTracker fcmock;
    private DB dbSession;
    protected static final String VIEW_ANGLE = "SummaVisible";

    @Before
    public void setUp() throws Exception {



        fcmock = mock(FedoraForUpdateTracker.class);
        //Collections for everybody
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(asSet(COLLECTION));
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
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call

        //Reconnect Objects
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(new Record(pid, VIEW_ANGLE, COLLECTION)); //This does not give null, so no saveAndUpdate
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));//Will be empty set
        mocks.verify(dbSession).getRecordsForPid(pid);  //Should return empty due to not flushed yet
        mocks.verify(fcmock).calcViewBundle(pid,VIEW_ANGLE,now);
        //No saveAndUpdate as the contained objects have not changed

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
        //ObjectDeleted
        mocks.verify(dbSession).beginTransaction();
        //ModifyStates
        mocks.verify(dbSession).getRecordsForPid(pid);
        mocks.verify(dbSession).saveOrUpdate(newRecord);
        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
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

        Date now = new Date();
        final String pid = "doms:test1";
        int key = 1;

        final Record keyedRecord = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid));
        addEntry("doms:test1");


        when(dbSession.recordExists(eq(keyedRecord)))
                .thenReturn(newRecord);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(newRecord));
        store.objectStateChanged("doms:test1", now, "A", 1);

        InOrder mocks = inOrder(dbSession, fcmock);
        //ObjectStateChanged
        mocks.verify(dbSession).beginTransaction();
        mocks.verify(fcmock).getCollections(pid, now);

        //ModifyStates
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid,now);
        mocks.verify(dbSession).recordExists(keyedRecord);
        mocks.verify(dbSession).saveOrUpdate(newRecord);
        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);

    }




    @Test
    public void testObjectRelationsChangedSameBundle() throws Exception {
        init();
        final String pid = "doms:test1";
        final int key = 1;

        final String child = "doms:test2";
        addEntry(pid, child);
        Date now = new Date();
        final Record keyedRecord = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid, child));
        when(dbSession.recordExists(eq(keyedRecord)))
                .thenReturn(newRecord);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(newRecord));


        store.objectRelationsChanged(pid, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);

        mocks.verify(dbSession).beginTransaction();

        mocks.verify(fcmock).isCurrentlyContentModel(pid, now);
        mocks.verify(fcmock).getCollections(pid, now);
        mocks.verify(fcmock).getState(pid,now);

        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(keyedRecord);

        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(pid);

        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);

        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testObjectRelationsChangedChangedBundle() throws Exception {
        init();
        final String pid = "doms:test1";
        final String child = "doms:test2";
        final int key = 1;

        addEntry(pid, child);

        Date now = new Date();
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);

        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid));


        final Record recordToSave = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid,
                                                                                                         child));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));


        store.objectRelationsChanged(pid, now, key);

        InOrder mocks = inOrder(dbSession, fcmock);


        mocks.verify(dbSession).beginTransaction();

        //Datastream RELSEXT changed
        mocks.verify(fcmock).isCurrentlyContentModel(pid, now);
        mocks.verify(fcmock).getCollections(pid, now);
        mocks.verify(fcmock).getState(pid,now);

        //backend.reconnectObjects
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(recordToLookup); //This returns true, so no save yet

        //Check for any old records which should be unlinked. There are none
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));
        //Find all the records affected by this change
        mocks.verify(dbSession).getRecordsForPid(pid);
        //Calc the new viewbundle for each of these
        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);
        //Save the change
        mocks.verify(dbSession).saveOrUpdate(recordToSave);


        //UpdateDAtes
        mocks.verify(dbSession).updateDates(pid, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testObjectRelationsChangedChildChanged() throws Exception {
        init();
        final String pid = "doms:test1";
        final String child = "doms:test2";
        final int key = 1;

        addEntry(pid, child);

        Date now = new Date();
        final Record recordToLookup = new Record(child, VIEW_ANGLE, COLLECTION);

        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid));


        final Record recordToSave = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid,
                                                                                                         child));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));
        when(dbSession.getRecordsForPid(child)).thenReturn(asSet(recordBefore));


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
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(child, asSet(String.class), asSet(COLLECTION));
        //Find all the records affected by this change
        mocks.verify(dbSession).getRecordsForPid(child);
        //Calc the new viewbundle for each of these
        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);
        //Save the change
        mocks.verify(dbSession).saveOrUpdate(recordToSave);


        //UpdateDAtes
        mocks.verify(dbSession).updateDates(child, now);
        //LatestKey
        mocks.verify(dbSession).setLatestKey(key);

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testLatestKeyInserted() throws Exception {
        init();
        long latestKey = store.getLatestKey();
        long newLatestKey = latestKey + 42;
        store.objectCreated("doms:test1", new Date(0L), newLatestKey);
        verify(dbSession).setLatestKey(newLatestKey);
    }
}
