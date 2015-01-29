package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.database.DomsUpdateTrackerUpdateTrackerPersistentStoreImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ObjectInfo;
import dk.statsbiblioteket.doms.updatetracker.improved.fedoraJms.FedoraMessageListener;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.net.MalformedURLException;
import java.util.List;

/**
 * This is the system that starts the persistent store and the jms listener and ties them together
 */
public class UpdateTrackingSystem {


    private static UpdateTrackerPersistentStore store;
    private static Fedora fedora;
    private static MessageConsumer consumer;


    public static synchronized void startup() throws MalformedURLException, JMSException {

        if (store != null && fedora != null && consumer != null){
            return;
        }

        String jmsurl = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.jms.brokeraddress", "tcp://localhost:61616");

        String jmssubject = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.jms.brokerqueue", "fedora.apim.update");


        String fedoraLocation = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.fedora.location", "http://localhost7880/fedora");


        String ecmLocation = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.ecm.location", "http://localhost:7880/ecm-service");

        String fedoraUser = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.fedora.user", "fedoraReadOnlyAdmin");

        String fedoraPass = ConfigCollection.getProperties().getProperty(
                        "dk.statsbiblioteket.doms.updatetracker.fedora.password", "FedoraReadOnlyPass");

        Credentials creds = new Credentials(fedoraUser, fedoraPass);

        //Start up the fedora connection
        fedora = new Fedora(creds,fedoraLocation,ecmLocation);

        //Start up the database
        store = new DomsUpdateTrackerUpdateTrackerPersistentStoreImpl(fedora);
        try {
            store.setUp();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //initialise the jms connection to Fedora
        consumer = initialiseJMS(jmsurl, jmssubject, fedoraUser,fedoraPass);

        //Tie it all together
        consumer.setMessageListener(new FedoraMessageListener(store));
    }

    public static UpdateTrackerPersistentStore getStore() {
        return store;
    }

    private static MessageConsumer initialiseJMS(String jmsurl, String jmssubject, String fedoraUser, String fedoraPass) throws JMSException {
        Connection connection = null;
        Destination destination = null;

        // Create the connection.
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(fedoraUser, fedoraPass, jmsurl);
        connection = connectionFactory.createConnection();
        connection.start();

        // Create the session
        Session session = connection.createSession(false,
                                                   Session.AUTO_ACKNOWLEDGE);

        destination = session.createTopic(jmssubject);

        // Create the Consumer
        MessageConsumer consumer = session.createConsumer(destination);
        return consumer;

    }

    public static void regenerateFromDOMS() throws UpdateTrackerStorageException, FedoraFailedException {

        //Clear the content
        store.clear();

        List<ObjectInfo> entryObjects = fedora.getAllEntryObjects();
        for (ObjectInfo entryObject : entryObjects) {
            store.objectCreated(entryObject.getObjectPid(),entryObject.getLastModified());
            if (entryObject.getState().equals("Active")){
                store.objectPublished(entryObject.getObjectPid(),entryObject.getLastModified());
            } else if (entryObject.getState().equals("Deleted")){
                store.objectDeleted(entryObject.getObjectPid(),entryObject.getLastModified());
            }
        }
    }
}