package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Date;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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
}