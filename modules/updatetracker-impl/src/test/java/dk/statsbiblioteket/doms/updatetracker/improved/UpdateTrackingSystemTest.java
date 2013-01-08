package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Entry;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.apache.commons.logging.impl.Log4JLogger;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/7/13
 * Time: 5:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackingSystemTest {
    private int offset;
    private int limit;

    @Test
    public void testRegenerateFromDOMS() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        ConfigCollection.addContextConfig(props);

        UpdateTrackingSystem.startup();
        UpdateTrackingSystem.getStore().dumpToStdOut();

        UpdateTrackingSystem.regenerateFromDOMS();
        UpdateTrackingSystem.getStore().lookup(new Date(0), "SummaVisible", 0, 1000, "A", false);


    }

    @Test
    public void testLookupDOMS() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        ConfigCollection.addContextConfig(props);

        UpdateTrackingSystem.startup();

        offset = 0;
        limit = 1000;
        while(true){
            List<Entry> results = UpdateTrackingSystem.getStore().lookup(new Date(0), "SummaVisible", offset, limit, "A", false);
            for (Entry result : results) {
                System.out.println(result);
            }
            if (results.size() == 0){
                break;
            } else {
                offset += limit;
            }
        }


    }
}
