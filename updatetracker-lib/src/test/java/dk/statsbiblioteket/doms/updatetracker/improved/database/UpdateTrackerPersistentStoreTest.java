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
        db.close();
        db.clear();
    }

    @Test
    public void testObjectCreated() throws Exception {
        Date now = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", now);
        List<Entry> list = db.lookup(now, "SummaVisible", 0, 100, null, false);
        assertEquals("To many objects, some should have been deleted", 1, list.size());

        list = db.lookup(new Date(), "SummaVisible", 0, 100, null, false);
        assertEquals("To many objects, some should have been deleted", 0, list.size());
    }

    @Test
    public void testObjectDeleted() throws Exception {

        Date old = new Date();
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", old);


        List<Entry> list = db.lookup(old, "SummaVisible", 0, 100, null, false);
        assertEquals("To many objects", 1, list.size());

        Date now = new Date();
        db.objectDeleted("doms:test1", now);

        list = db.lookup(now, "SummaVisible", 0, 100, null, false);
        assertEquals("To many objects", 1, list.size());

        list = db.lookup(new Date(now.getTime() + 1), "SummaVisible", 0, 100, null, false);
        assertEquals("To many objects", 0, list.size());
    }

    @Test
    public void testObjectRelationsChanged() throws Exception {
        Date frozen = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", frozen);
        db.objectCreated("doms:test2", frozen);
        fcmock.addEntry("doms:test1", "doms:test2");

        List<Entry> list = db.lookup(frozen, "SummaVisible", 0, 100, null, false);
        assertEquals(1, list.size());
        assertEquals(list.get(0).getDateForChange().getTime(), frozen.getTime());

        Date flow = new Date();
        db.objectRelationsChanged("doms:test1", flow);

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, false);
        assertEquals(1, list.size());
        assertEquals(flow.getTime(), list.get(0).getDateForChange().getTime());

        Thread.sleep(1000);
        Date flow2 = new Date();

        db.datastreamChanged("doms:test2", flow2, null);

        list = db.lookup(frozen, "SummaVisible", 0, 100, null, false);
        assertEquals(1, list.size());
        assertEquals(flow2.getTime(), list.get(0).getDateForChange().getTime());

        Date flow3 = new Date();

        db.objectStateChanged("doms:test1", flow3, "A");
        list = db.lookup(frozen, "SummaVisible", 0, 100, null, false);
        assertEquals(2, list.size());
        assertEquals(list.get(0).getDateForChange().getTime(), flow3.getTime());
        assertEquals(list.get(1).getDateForChange().getTime(), flow3.getTime());
    }

    @Test
    public void testObjectPublished() throws Exception {
        Date frozen = new Date(0);
        fcmock.addEntry("doms:test1");
        db.objectCreated("doms:test1", frozen);

        List<Entry> list = db.lookup(frozen, "SummaVisible", 0, 100, "A", true);
        assertEquals("Wrong number of objects", list.size(), 0);

        Thread.sleep(1000);
        final Date published = new Date();
        db.objectStateChanged("doms:test1", published, "A");
        list = db.lookup(frozen, "SummaVisible", 0, 100, "A", true);
        assertEquals("Wrong number of objects", list.size(), 1);
        assertEquals(published.getTime(), list.get(0).getDateForChange().getTime());
        assertEquals(list.get(0).getState(), "A");
    }
}
