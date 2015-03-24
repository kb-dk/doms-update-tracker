package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * This is an forever test, used to see if the worklog can actually be replayed.
 */
@Ignore("This test is to manually test against a vagrant")
public class WorkLogPollerTest {

    long ONEHOUR = 60*60*1000;
    @Test
    public void testGetFedoraEvents() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        UpdateTrackingConfig config = new UpdateTrackingConfig(props);
        UpdateTrackingSystem system = new UpdateTrackingSystem(config);
        Thread.sleep(ONEHOUR);

    }
}