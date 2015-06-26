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
    public void testGetFedoraEvents() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));



        Database.init(props.getProperty("fedora.worklog.database.URL"),
                props.getProperty("fedora.worklog.database.username"),
                props.getProperty("fedora.worklog.database.password"));
        Database.executeSQL(new File(Thread.currentThread().getContextClassLoader().getResource("updatetrackerLogs.ddl").toURI()));
        UpdateTrackingConfig config = new UpdateTrackingConfig(props);

        WorkLogPollDAO workLogPollDAO = new WorkLogPollDAO(
                config.getFedoraDatabaseDriver(),
                config.getFedoraDatabaseURL(),
                config.getFedoraDatabaseUsername(),
                config.getFedoraDatabasePassword());
        UpdateTrackerPersistentStore persistentStore = mock(UpdateTrackerPersistentStoreImpl.class);
        WorkLogPollTask pollTask = new WorkLogPollTask(workLogPollDAO, persistentStore, 10, 1000);
        pollTask.run();
        verify(persistentStore).getLatestKey();
        verifyNoMoreInteractions(persistentStore);
        reset(persistentStore);
        Thread.sleep(1000);
        pollTask.run();
        verify(persistentStore).getLatestKey();
        verify(persistentStore).datastreamChanged(eq("doms:testpid"), any(Date.class), eq("EVENTS"), eq(1L));
        verifyNoMoreInteractions(persistentStore);

    }
}