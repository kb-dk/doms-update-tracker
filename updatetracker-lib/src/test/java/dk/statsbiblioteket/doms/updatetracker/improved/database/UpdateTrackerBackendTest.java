package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.DBFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.database.dao.StatelessDB;
import dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import org.hibernate.Transaction;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.Utils.asSet;
import static dk.statsbiblioteket.doms.updatetracker.improved.database.datastructures.Record.State.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        when(fcmock.getCollections(anyString(), any(Date.class))).thenReturn(asSet(COLLECTION));
        //No entry objects or view stuff until initialised
        when(fcmock.getEntryAngles(anyString(), any(Date.class))).thenReturn(Collections.<String>emptySet());
        uptrack = new UpdateTrackerBackend(fcmock, 10000L);

        DBFactory dbfac = mock(DBFactory.class);
        dbSession = mock(DB.class);
        Transaction transaction = mock(Transaction.class);
        when(dbfac.getInstance()).thenReturn(dbSession);
        when(dbSession.beginTransaction()).thenReturn(transaction);
        StatelessDB statelessDB = mock(StatelessDB.class);
        when(dbfac.getStatelessDB()).thenReturn(statelessDB);

    }


    @Test
    public void testModifyState() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);

        uptrack.modifyState(pid,now,COLLECTION, INACTIVE,dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(new Record(pid, VIEW_ANGLE, COLLECTION));
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid));

        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }

    @Test
    public void testModifyStateActive() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);

        uptrack.modifyState(pid, now, COLLECTION, ACTIVE, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(dbSession).getAllRecordsWithThisEntryPid(pid);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(new Record(pid, VIEW_ANGLE, COLLECTION));
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, now, now, null, null, asSet(pid));

        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession,fcmock);
    }


    @Test
    public void testModifyStateDeleted() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));


        uptrack.modifyState(pid, now, COLLECTION, DELETED, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);

        mocks.verify(dbSession).getRecordsForPid(pid);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, null, now, null, asSet(String.class));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testModifyStateDeletedWithChildren() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid,"doms:pid2","doms:pid3");
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid,
                                                                                                                 "doms:pid2",
                                                                                                                 "doms:pid3"));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));

        uptrack.modifyState(pid, now, COLLECTION, DELETED, dbSession);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(dbSession).getRecordsForPid(pid);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, null, now, null, asSet(String.class));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }


    @Test
    public void testReconnectObjectsOneAdded() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid, "doms:pid2");
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));

        uptrack.reconnectObjects(pid, now, dbSession, asSet(COLLECTION), DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(fcmock).getEntryAngles(pid,now);
        mocks.verify(dbSession).recordExists(recordToLookup);
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(pid);
        mocks.verify(fcmock).calcViewBundle(pid,VIEW_ANGLE,now);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid,
                                                                                                      "doms:pid2"));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }

    @Test
    public void testReconnectObjectsOneRemoved() throws Exception {
        String pid = "doms:pid1";
        Date now = new Date();

        addEntry(pid);
        final Record recordToLookup = new Record(pid, VIEW_ANGLE, COLLECTION);
        final Record recordBefore = new Record(pid, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid, "doms:pid2"));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(recordBefore);
        when(dbSession.getRecordsForPid(pid)).thenReturn(asSet(recordBefore));

        uptrack.reconnectObjects(pid, now, dbSession, asSet(COLLECTION), DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(fcmock).getEntryAngles(pid, now);
        mocks.verify(dbSession).recordExists(recordToLookup);
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid, asSet(VIEW_ANGLE), asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(pid);
        mocks.verify(fcmock).calcViewBundle(pid, VIEW_ANGLE, now);
        final Record newRecord = new Record(pid, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }

    @Test
    public void testReconnectObjectsChildInTwoRecords() throws Exception {
        String pid1 = "doms:entry1";
        String pid2 = "doms:entry2";
        String child1 = "doms:child1";
        Date now = new Date();

        addEntry(pid1,child1);
        addEntry(pid2,child1);
        final Record recordToLookup = new Record(pid1, VIEW_ANGLE, COLLECTION);
        final Record record1Before = new Record(pid1, VIEW_ANGLE, COLLECTION, null, new Date(1), null, null, asSet(pid1));
        final Record record2Before = new Record(pid2,
                                               VIEW_ANGLE,
                                               COLLECTION,
                                               null,
                                               new Date(1),
                                               null,
                                               null,
                                               asSet(pid2,child1));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(record1Before);
        when(dbSession.getRecordsForPid(child1)).thenReturn(asSet(record1Before,record2Before));

        uptrack.reconnectObjects(pid1, now, dbSession, asSet(COLLECTION), DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(fcmock).getEntryAngles(pid1, now);
        mocks.verify(dbSession).recordExists(recordToLookup);
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(pid1, asSet(VIEW_ANGLE), asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(pid1);
        mocks.verify(fcmock).calcViewBundle(pid1, VIEW_ANGLE, now);
        final Record newRecord = new Record(pid1, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid1,child1));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call
        verifyNoMoreInteractions(dbSession, fcmock);
    }


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
                                                asSet(pid1));
        final Record record2Before = new Record(pid2,
                                                VIEW_ANGLE,
                                                COLLECTION,
                                                null,
                                                new Date(1),
                                                null,
                                                null,
                                                asSet(pid2, child1));

        when(dbSession.recordExists(eq(recordToLookup)))
                .thenReturn(null);
        when(dbSession.getRecordsForPid(child1)).thenReturn(asSet(record1Before, record2Before));

        uptrack.reconnectObjects(child1, now, dbSession, asSet(COLLECTION), DELETED);

        InOrder mocks = inOrder(dbSession, fcmock);
        mocks.verify(fcmock).getEntryAngles(child1, now);
        mocks.verify(dbSession).getRecordsNotInTheseCollectionsAndViewAngles(child1,
                                                                             asSet(String.class),
                                                                             asSet(COLLECTION));
        mocks.verify(dbSession).getRecordsForPid(child1);
        mocks.verify(fcmock).calcViewBundle(pid2, VIEW_ANGLE, now);
        mocks.verify(fcmock).calcViewBundle(pid1, VIEW_ANGLE, now);
        final Record newRecord = new Record(pid1, VIEW_ANGLE, COLLECTION, null, now, null, null, asSet(pid1, child1));
        mocks.verify(dbSession).saveOrUpdate(newRecord);  //Because RecordExists give null in the first call

        verifyNoMoreInteractions(dbSession, fcmock);
    }


    private void addEntry(String pid, String... contained) throws FedoraFailedException {
        when(fcmock.getEntryAngles(eq(pid), any(Date.class))).thenReturn(asSet(VIEW_ANGLE));
        when(fcmock.getState(eq(pid), any(Date.class))).thenReturn(Record.State.INACTIVE);
        List<String> objects = new ArrayList<String>(Arrays.asList(contained));
        objects.add(pid);
        when(fcmock.calcViewBundle(eq(pid), eq(VIEW_ANGLE), any(Date.class))).thenReturn(new ViewBundle(pid,
                                                                                                        VIEW_ANGLE,
                                                                                                        objects));
    }
}