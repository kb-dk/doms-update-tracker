package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.List;


import static junit.framework.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 5/3/11
 * Time: 9:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackerPersistentStoreTest {

    UpdateTrackerPersistentStore db;
    FedoraMockup fcmock;

    public UpdateTrackerPersistentStoreTest() throws MalformedURLException {
        fcmock = new FedoraMockup(new Credentials("user", "pass"), null, null);
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
    public void testObjectCreatedBasic() throws Exception {
        Date now = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", now);
        List<Entry> list = db.lookup(now, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        list = db.lookup(new Date(), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 0, list.size());
    }

    @Test
    public void testObjectCreatedBeforeAndAfter() throws Exception {

        Date start = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", start);
        List<Entry> list = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        List<Entry> list2 = db.lookup(new Date(start.getTime()-1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list2.size());

        List<Entry> list3 = db.lookup(new Date(start.getTime() + 1), "SummaVisible", 0, 100, null,
                                             "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 0, list3.size());
    }

    @Test
    public void testObjectCreatedMany() throws Exception {

        Date start = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", start);



        List<Entry> list = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        Date test3 = new Date();
        fcmock.addEntry("doms:test3");
        db.objectCreated("doms:test3", test3);

        List<Entry> list2 = db.lookup(start, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects, some should have been deleted", 2, list2.size());
    }


    @Test
    public void testObjectDeletedBasic() throws Exception {

        Date test1Create = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Entry> list = db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        list = db.lookup(test1Delete, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 1, list.size());

        list = db.lookup(new Date(test1Delete.getTime() + 1), "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("To many objects", 0, list.size());
    }


    @Test
    public void testObjectDeletedMultiple() throws Exception {

        Date test1Create = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test2Create = new Date();
        fcmock.addEntry("doms:test2");
        db.objectCreated("doms:test2", test2Create);
        assertEquals("To many objects", 2, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test2Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        //Test how many Inactive objects there are
        final List<Entry> inactive = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("To many objects", 1, inactive.size());
        assertEquals("doms:test2", inactive.get(0).getEntryPid());

        final List<Entry> deleted = db.lookup(test1Delete, "SummaVisible", 0, 100, "D", "doms:Root_Collection");
        assertEquals("To many objects", 1, deleted.size());
        assertEquals("D", deleted.get(0).getState());
        assertEquals("doms:test1", deleted.get(0).getEntryPid());
    }

    @Test
    public void testObjectRessurection() throws Exception {

        Date test1Create = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());

        Date test1Delete = new Date();
        db.objectDeleted("doms:test1", test1Delete);

        //Test how many Inactive objects there are
        final List<Entry> inactive = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("To many objects", 0, inactive.size());
        final List<Entry> deleted = db.lookup(test1Delete, "SummaVisible", 0, 100, "D", "doms:Root_Collection");
        assertEquals("To many objects", 1, deleted.size());
        assertEquals("D", deleted.get(0).getState());
        assertEquals("doms:test1", deleted.get(0).getEntryPid());

        Date test1CreateAgain = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1CreateAgain);
        assertEquals("To many objects", 2, db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test1CreateAgain, "SummaVisible", 0, 100, null, "doms:Root_Collection").size());
        assertEquals("To many objects", 1, db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection").size());
    }

    @Test
    public void testObjectPublished() throws Exception {
        Date test1Create = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Entry> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "I");
    }


    @Test
    public void testObjectPublishedAndUnpublished() throws Exception {
        Date test1Create = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Entry> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "A");

        //After publish, the object still exist as Inactive
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "I");

        final Date test1Unpublished = new Date();
        db.objectStateChanged("doms:test1", test1Unpublished, "I");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Unpublished.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "I");
    }


    @Test
    public void testObjectPublishedAndDeleted() throws Exception {
        Date test1Create = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", test1Create);

        List<Entry> list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 0);

        final Date test1Published = new Date();
        db.objectStateChanged("doms:test1", test1Published, "A");

        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "A");

        //After publish, the object still exist as Inactive
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        //And with the published timestamp, even
        assertEquals(test1Published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "I");

        final Date test1Deleted = new Date();
        db.objectStateChanged("doms:test1", test1Deleted, "D");

        //After delete, the object is no longer published
        list = db.lookup(test1Create, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals("Wrong number of objects", 0,list.size());

        list = db.lookup(test1Create, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(test1Deleted.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "D");
    }


    @Test
    public void testObjectRelationsChangedBasic() throws Exception {
        Date frozen = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", frozen);
        db.objectCreated("doms:test2", frozen);
        fcmock.addEntry("doms:test1", "doms:test2");

        List<Entry> list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(list.get(0).getDateForChange().getTime(), frozen.getTime());

        Date flow = new Date();
        db.objectRelationsChanged("doms:test1", flow);

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(flow.getTime(), list.get(0).getDateForChange().getTime());

        Thread.sleep(1000);
        Date flow2 = new Date();

        db.datastreamChanged("doms:test2", flow2, null);

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(flow2.getTime(), list.get(0).getDateForChange().getTime());

        Date flow3 = new Date();

        db.objectStateChanged("doms:test1", flow3, "A");
        list = db.lookup(frozen, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(2, list.size());
        assertEquals(list.get(0).getDateForChange().getTime(), flow3.getTime());
        assertEquals(list.get(1).getDateForChange().getTime(), flow3.getTime());
    }


    @Test
    public void testObjectRelationsChangedDeep() throws Exception {
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        fcmock.addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Entry> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getDateForChange().getTime());

        Date test3RelChange = new Date();

        db.datastreamChanged("doms:test3", test3RelChange, null);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test3RelChange.getTime(), list.get(0).getDateForChange().getTime());


    }

    @Test
    public void testObjectRelationsChangedPublished() throws Exception {
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        fcmock.addEntry("doms:test1", "doms:test2", "doms:test3");

        //The entry was added after ingest, so the objects should not be in the index
        List<Entry> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(0, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1RelChange.getTime(), list.get(0).getDateForChange().getTime());

        Date test1Publish = new Date();
        db.objectStateChanged("doms:test1", test1Publish, "A");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1Publish.getTime(), list.get(0).getDateForChange().getTime());

        Date test2Publish = new Date();
        db.objectStateChanged("doms:test2", test2Publish, "A");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test2Publish.getTime(), list.get(0).getDateForChange().getTime());

        //As long as the entry is published, the state of the minors do not matter
        Date test2unPublish = new Date();
        db.objectStateChanged("doms:test2", test2unPublish, "I");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test2unPublish.getTime(), list.get(0).getDateForChange().getTime());

        Date test1unPublish = new Date();
        db.objectStateChanged("doms:test1", test1unPublish, "I");
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "A", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test2unPublish.getTime(), list.get(0).getDateForChange().getTime());
        list = db.lookup(ingest1, "SummaVisible", 0, 100, "I", "doms:Root_Collection");
        assertEquals(1, list.size());
        assertEquals(test1unPublish.getTime(), list.get(0).getDateForChange().getTime());
    }


    @Test
    public void testObjectRelationsChangedDeepMultiple() throws Exception {
        Date ingest1 = new Date(0);
        Date ingest2 = new Date(10);
        Date ingest3 = new Date(20);
        db.objectCreated("doms:test1", ingest1);
        db.objectCreated("doms:test2", ingest2);
        db.objectCreated("doms:test3", ingest3);
        fcmock.addEntry("doms:test4", "doms:test5", "doms:test6");
        db.objectCreated("doms:test4", ingest1);
        db.objectCreated("doms:test5", ingest2);
        db.objectCreated("doms:test6", ingest3);

        fcmock.addEntry("doms:test1", "doms:test2", "doms:test3");


        //The entry was added after ingest, so the objects should not be in the index
        List<Entry> list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(1, list.size());

        Date test1RelChange = new Date();
        db.objectRelationsChanged("doms:test1", test1RelChange);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(2, list.size());
        assertEquals(test1RelChange.getTime(), list.get(1).getDateForChange().getTime());

        Date test3DatastreamChange = new Date();

        db.datastreamChanged("doms:test3", test3DatastreamChange, null);

        list = db.lookup(ingest1, "SummaVisible", 0, 100, null, "doms:Root_Collection");
        assertEquals(2, list.size());
        assertEquals(test3DatastreamChange.getTime(), list.get(1).getDateForChange().getTime());
    }
}
