package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.updatetracker.webservice.PidDatePidPid;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.junit.Ignore;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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

        Date startDate = new SimpleDateFormat("yyyy-MM-dd").parse("2012-03-01");
        Date before = new Date();
        List<PidDatePidPid> oldest = service.listObjectsChangedSince("doms:RadioTV_Collection",
                "SummaVisible",
                startDate.getTime(),
                null,
                0,
                10000);
        Date after = new Date();
        System.out.println((after.getTime()-before.getTime()));

        for (PidDatePidPid pidDatePidPid : oldest) {
            System.out.println(pidDatePidPid.getCollectionPid());
            System.out.println(pidDatePidPid.getPid());
            System.out.println(pidDatePidPid.getLastChangedTime());
        }
    }
}
