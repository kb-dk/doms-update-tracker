package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.updatetracker.webservice.PidDatePidPid;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.junit.Ignore;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 7/3/13
 * Time: 1:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateTrackerWebserviceLibTest {
    @org.junit.Before
    public void setUp() throws Exception {

    }

    @org.junit.After
    public void tearDown() throws Exception {

    }


    @org.junit.Test
    @Ignore
    public void testGetModifiedObjects() throws Exception {
        ConfigCollection.getProperties().setProperty(
                       "dk.statsbiblioteket.doms.updatetracker.fedoralocation",
                "http://alhena:7980/fedora");
        UpdateTrackerWebserviceLib service = new UpdateTrackerWebserviceLib(new CredentialsGenerator() {
            @Override
            public Credentials getCredentials() {
                Credentials creds = new Credentials("fedoraAdmin", "fedoraAdminPass");
                return creds;
            }
        });

        List<PidDatePidPid> oldest = service.listObjectsChangedSince("doms:RadioTV_Collection", "SummaVisible", 0, null, 0, 100);
        for (PidDatePidPid pidDatePidPid : oldest) {
            System.out.println(pidDatePidPid.getCollectionPid());
            System.out.println(pidDatePidPid.getPid());
            System.out.println(pidDatePidPid.getLastChangedTime());
        }
    }
}
