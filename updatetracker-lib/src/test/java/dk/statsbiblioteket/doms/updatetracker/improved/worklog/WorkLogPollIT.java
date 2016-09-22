package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.util.Date;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WorkLogPollIT {

    @Test
    public void testGetEventFromDockerWorklog() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        UpdateTrackingConfig config = new UpdateTrackingConfig(props);

        //Create the worklog table in the docker postgres database
        Database.init(config.getFedoraDatabaseURL(),
                config.getFedoraDatabaseUsername(),
                config.getFedoraDatabasePassword());
        Database.executeSQL(new File(Thread.currentThread().getContextClassLoader().getResource("updatetrackerLogs.ddl").toURI()));
        Database.executeSQL("INSERT INTO updatetrackerlogs (key, pid, happened, method, param) VALUES (1, 'doms:testpid', CURRENT_TIMESTAMP, 'modifyDatastreamByReference', 'EVENTS');");

        //Mock the thing that the workLogpoller calls
        UpdateTrackerPersistentStore persistentStore = mock(UpdateTrackerPersistentStoreImpl.class);

        //Create the worklog dao
        WorkLogPollDAO workLogPollDAO = new WorkLogPollDAO(
                config.getFedoraDatabaseDriver(),
                config.getFedoraDatabaseURL(),
                config.getFedoraDatabaseUsername(),
                config.getFedoraDatabasePassword());

        //Create the poll task, notice the delay of 1 second. Entries younger than that are ignorede
        WorkLogPollTask pollTask = new WorkLogPollTask(workLogPollDAO, persistentStore, 10, 1000);
        pollTask.run();
        //We expect no work units to be found, as the recently created entry should be to young
        verify(persistentStore).getLatestKey();
        verifyNoMoreInteractions(persistentStore);

        //Reset the mock so the verifys start from 0 again
        reset(persistentStore);
        //Sleep for long enough that the one event in the worklog is old enough
        Thread.sleep(1000);
        //Start the task again
        pollTask.run();
        verify(persistentStore).getLatestKey();
        //This time we expect the event to be found
        verify(persistentStore).datastreamChanged(eq("doms:testpid"), any(Date.class), eq("EVENTS"), eq(1L));
        verifyNoMoreInteractions(persistentStore);

    }


    /**
     * This test tests that even if the worklogunit key and timestamp are not in sync, the events are processed
     * in key order.
     * Key and timestamp gets out of sync when we do ingest operations, as these register the event AFTER the operation
     * while all other operations register before.
     * @throws Exception
     */
    @Test
    public void testContiniousNumbers() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        UpdateTrackingConfig config = new UpdateTrackingConfig(props);

        //Create the worklog table in the docker postgres database
        Database.init(config.getFedoraDatabaseURL(),
                config.getFedoraDatabaseUsername(),
                config.getFedoraDatabasePassword());
        Database.executeSQL(new File(Thread.currentThread().getContextClassLoader().getResource("updatetrackerLogs.ddl").toURI()));

        //Notice that we use a different ordering for key and happened here. This have been known to happen with ingest operations
        Database.executeSQL(
                "INSERT INTO public.updatetrackerlogs (key, pid, happened, method, param) VALUES (3, 'doms:testpid', NOW(), 'ingest', NULL);\n");
        Thread.sleep(1000);
        Database.executeSQL(
                "INSERT INTO public.updatetrackerlogs (key, pid, happened, method, param) VALUES (1, 'doms:testpid', NOW(), 'modifyDatastreamByReference', 'EVENTS');\n");
        Thread.sleep(1000);
        Database.executeSQL(
                "INSERT INTO public.updatetrackerlogs (key, pid, happened, method, param) VALUES (2, 'doms:testpid', NOW(), 'modifyDatastreamByReference', 'EVENTS');\n");
        Thread.sleep(1000);

        //Mock the thing that the workLogpoller calls
        UpdateTrackerPersistentStore persistentStore = mock(UpdateTrackerPersistentStoreImpl.class);

        //Create the worklog dao
        WorkLogPollDAO workLogPollDAO = new WorkLogPollDAO(
                config.getFedoraDatabaseDriver(),
                config.getFedoraDatabaseURL(),
                config.getFedoraDatabaseUsername(),
                config.getFedoraDatabasePassword());

        //Create the poll task, notice the delay of 1 second. Entries younger than that are ignorede
        WorkLogPollTask pollTask = new WorkLogPollTask(workLogPollDAO, persistentStore, 10, 100);

        pollTask.run();
        InOrder order = inOrder(persistentStore);
        verify(persistentStore).getLatestKey();
        //Verify that the operations are found in order, even when the happened is out of order
        order.verify(persistentStore).datastreamChanged(eq("doms:testpid"), any(Date.class), eq("EVENTS"), eq(1L));
        order.verify(persistentStore).datastreamChanged(eq("doms:testpid"), any(Date.class), eq("EVENTS"), eq(2L));
        order.verify(persistentStore).objectCreated(eq("doms:testpid"), any(Date.class), eq(3L));
        verifyNoMoreInteractions(persistentStore);

    }
}