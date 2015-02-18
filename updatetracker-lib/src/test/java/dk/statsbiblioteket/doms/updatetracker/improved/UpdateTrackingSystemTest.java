package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.database.Record;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.junit.Ignore;
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
    @Ignore
    public void testGetFromDOMS() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        ConfigCollection.addContextConfig(props);

        UpdateTrackingSystem.startup();
        Thread.sleep(120*1000);

        UpdateTrackingSystem.getStore().dumpToStdOut();

    }

    @Test
    public void testLookupDOMS() throws Exception {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("test.properties"));
        ConfigCollection.addContextConfig(props);

        UpdateTrackingSystem.startup();

        offset = 0;
        limit = 10;
        while(true){
            List<Record> results = UpdateTrackingSystem.getStore().lookup(new Date(0), "SummaVisible", offset, limit, "I",
                                                                                "doms:Newspaper_Collection");
            for (Record result : results) {
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
