package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.util.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerDAO.asSet;
import static java.util.Collections.emptyList;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateTrackerPersistentStoreTest {

    private final String collection = "doms:Root_Collection";
    UpdateTrackerPersistentStore db;
    FedoraForUpdateTracker fedora;
    private File configFile;

    @Before
    public void setUp() throws Exception {
        configFile = new File(Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hibernate.cfg.xml")
                                    .toURI());

        fedora = mock(FedoraForUpdateTracker.class);
        //Collections for everybody
        when(fedora.getCollections(anyString(), any(Date.class))).thenReturn(asSet(collection));
        //No entry objects or view stuff until initialised
        when(fedora.getEntryAngles(anyString(), any(Date.class))).thenReturn(emptyList());
        db = new UpdateTrackerPersistentStoreImpl(configFile,fedora);
    }


    @After
    public void tearDown() throws Exception {
        db.close();
    }

    @Test
    public void testObjectCreatedBasic() throws Exception {
        init();
        Date now = new Date();
        final String pid = "doms:test1";
        addEntry(pid);
        db.objectCreated(pid, now);
        List<Record> list = db.lookup(now, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        list = db.lookup(new Date(), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 0, list.size());
    }

    private void init() throws Exception {
        //tearDown();
        //setUp();
    }

    private void addEntry(String pid, String... contained) throws FedoraFailedException {
        final String viewAngle = "SummaVisible";
        when(fedora.getEntryAngles(eq(pid), any(Date.class))).thenReturn(Arrays.asList(viewAngle));
        List<String> objects = new ArrayList<>(Arrays.asList(contained));
        objects.add(pid);
        when(fedora.calcViewBundle(eq(pid),eq(viewAngle),any(Date.class))).thenReturn(new ViewBundle(pid,
                                                                                                               viewAngle,
                                                                                                               objects));
    }

    private void removeEntry(String pid) throws FedoraFailedException {
        final String viewAngle = "SummaVisible";
        when(fedora.getEntryAngles(eq(pid), any(Date.class))).thenThrow(new FedoraFailedException("Object not found"));
        when(fedora.calcViewBundle(eq(pid), eq(viewAngle), any(Date.class))).thenThrow(new FedoraFailedException("Object not found"));
    }


    @Test
    public void testObjectCreatedBeforeAndAfter() throws Exception {
        init();
        Date start = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", start);
        List<Record> list = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        List<Record> list2 = db.lookup(new Date(start.getTime()-1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list2.size());

        List<Record> list3 = db.lookup(new Date(start.getTime() + 1), "SummaVisible", 0, 100, null,
                                             "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 0, list3.size());
    }

    @Test
    public void testObjectCreatedMany() throws Exception {
        init();
        Date start = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", start);



        List<Record> list = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        Date test3 = new Date();
        addEntry("doms:test3");
        db.objectCreated("doms:test3", test3);

        List<Record> list2 = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 2, list2.size());
    }


    @Test
    public void testObjectDeletedBasic() throws Exception {
        init();
        Date test1Create = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Record> list = db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        list = db.lookup(test1Delete, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        list = db.lookup(new Date(test1Delete.getTime() + 1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 0, list.size());
    }

    @Test
    public void testObjectPurgedBasic() throws Exception {
        init();
        Date test1Create = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Record> list = db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        Date test1Delete = new Date();
        removeEntry("doms:test1");
        db.objectDeleted("doms:test1", test1Delete);

        list = db.lookup(test1Delete, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        list = db.lookup(new Date(test1Delete.getTime() + 1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 0, list.size());
    }


    @Test
    public void testObjectDeletedMultiple() throws Exception {
        init();
        Date test1Create = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test2Create = new Date();
        addEntry("doms:test2");
        db.objectCreated("doms:test2", test2Create);
        assertEquals("To many objects", 2, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test2Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        //Test how many Inactive objects there are
        final List<Record> inactive = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("To many objects", 2, inactive.size());
        assertEquals("doms:test2", inactive.get(0).getEntryPid());
        assertEquals("doms:test1", inactive.get(1).getEntryPid());
        assertEquals(Record.State.DELETED, inactive.get(1).getState());

        final List<Record> deleted = db.lookup(test1Delete, "SummaVisible", 0, 100, "D", "doms:Root_Collection");
        assertEquals("To many objects", 1, deleted.size());
        assertEquals(test1Delete.getTime(), deleted.get(0).getDeleted().getTime());
        assertEquals("doms:test1", deleted.get(0).getEntryPid());
        assertEquals(Record.State.DELETED, deleted.get(0).getState());
    }

    @Test
    public void testObjectRessurection() throws Exception {
        init();
        Date test1Create = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        //Test how many Inactive objects there are
        final List<Record> inactive = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("To many objects", 0, inactive.stream().filter(record -> record.getState() != Record.State.DELETED).count());
        final List<Record> deleted = db.lookup(test1Delete, "SummaVisible", 0, 100, "D", "doms:Root_Collection");
        assertEquals("To many objects", 1, deleted.size());
        assertEquals(test1Delete.getTime(), deleted.get(0).getDeleted().getTime());
        assertEquals("doms:test1", deleted.get(0).getEntryPid());

        Date test1CreateAgain = new Date();
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1CreateAgain);
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test1CreateAgain, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection").size());
    }

    @Test
    public void testObjectPublished() throws Exception {
        init();
        Date test1Create = new Date(0);
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Record> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

    }


    @Test
    public void testObjectPublishedAndUnpublished() throws Exception {
        init();
        Date test1Create = new Date(0);
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Record> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

        final Date test1Unpublished = new Date();
        db.objectStateChanged("doms:test1", test1Unpublished, "I");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Unpublished.getTime(), list.get(0).getInactive().getTime());
    }


    @Test
    public void testObjectPublishedAndDeleted() throws Exception {
        init();
        Date test1Create = new Date(0);
        addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Record> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getActive().getTime());

        //After publish, the object still exist as Inactive
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getInactive().getTime());

        final Date test1Deleted = new Date();
        db.objectStateChanged("doms:test1", test1Deleted, "D");

        //After delete, the object is no longer published
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", 1,list.size());
        assertEquals(list.get(0).getState(), Record.State.DELETED);

        list = db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Deleted.getTime(), list.get(0).getDeleted().getTime());
    }


    @Test
    public void testObjectRelationsChangedBasic() throws Exception {
        init();
        Date frozen = new Date(0);
        addEntry("doms:test1");
        db.objectCreated("doms:test1", frozen);
        db.objectCreated("doms:test2", frozen);
        addEntry("doms:test1", "doms:test2");

        List<Record> list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(list.get(0).getInactive().getTime(), frozen.getTime());

        Date flow = new Date();
        db.objectRelationsChanged("doms:test1", flow);

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(flow.getTime(), list.get(0).getInactive().getTime());

        Thread.sleep(1000);
        Date flow2 = new Date();

        db.datastreamChanged("doms:test2", flow2, "Something");

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(flow2.getTime(), list.get(0).getInactive().getTime());

        Date flow3 = new Date();

        db.objectStateChanged("doms:test1", flow3, "A");
        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(list.get(0).getInactive().getTime(), flow3.getTime());
        assertEquals(list.get(0).getActive().getTime(), flow3.getTime());
    }


    @Test
    public void testObjectRelationsChangedDeep() throws Exception {
        init();
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getInactive().getTime());

        Date test3RelChange = new Date();

        db.datastreamChanged("doms:test3", test3RelChange, "DSID");

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test3RelChange.getTime(), list.get(0).getInactive().getTime());


    }

    @Test
    public void testObjectRelationsChangedPublished() throws Exception {
        init();
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getInactive().getTime());

        Date test1Publish = new Date();
        db.objectStateChanged("doms:test1", test1Publish, "A");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1Publish.getTime(), list.get(0).getActive().getTime());

        Date test2Publish = new Date();
        db.objectStateChanged("doms:test2", test2Publish, "A");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test2Publish.getTime(), list.get(0).getActive().getTime());

        //As long as the entry is published, the state of the minors do not matter
        Date test2unPublish = new Date();
        db.objectStateChanged("doms:test2", test2unPublish, "I");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test2unPublish.getTime(), list.get(0).getInactive().getTime());

        Date test1unPublish = new Date();
        db.objectStateChanged("doms:test1", test1unPublish, "I");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1unPublish.getTime(), list.get(0).getInactive().getTime());
        assertEquals(test2unPublish.getTime(), list.get(0).getActive().getTime());
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1unPublish.getTime(), list.get(0).getInactive().getTime());
    }


    @Test
    public void testObjectRelationsChangedDeepMultiple() throws Exception {
        init();
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        addEntry("doms:test4", "doms:test5", "doms:test6");
        db.objectCreated("doms:test4", ingest1);
        db.objectCreated("doms:test5", ingest2);
        db.objectCreated("doms:test6", ingest3);

        addEntry("doms:test1", "doms:test2", "doms:test3");


        //The entry was added after ingest, so the objects should not be in the index
        List<Record> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(2, list.size());
        assertEquals(test1RelChange.getTime(), list.get(1).getInactive().getTime());

        Date test3DatastreamChange = new Date();

        db.datastreamChanged("doms:test3", test3DatastreamChange, "DSID");

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(2, list.size());
        assertEquals(test3DatastreamChange.getTime(), list.get(1).getInactive().getTime());
    }
}
