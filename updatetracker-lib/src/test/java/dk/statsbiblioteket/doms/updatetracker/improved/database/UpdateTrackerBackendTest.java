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
    }

    @Test
    public void testReconnectObjects() throws Exception {

    }

    @Test
    public void testUpdateDates() throws Exception {

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