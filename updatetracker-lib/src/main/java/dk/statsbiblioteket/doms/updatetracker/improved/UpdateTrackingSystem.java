package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedoraJms.FedoraMessageListener;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.doms.webservices.configuration.ConfigCollection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.net.MalformedURLException;

/**
 * This is the system that starts the persistent store and the jms listener and ties them together
 */
public class UpdateTrackingSystem {


    private static UpdateTrackerPersistentStore store;
    private static Fedora fedora;
    private static MessageConsumer consumer;


    public static synchronized void startup() throws JMSException, MalformedURLException {

        if (store != null && fedora != null && consumer != null){
            return;
        }

        String jmsurl = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.jms.brokeraddress", "tcp://localhost:61616");

        String jmssubject = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.jms.brokerqueue", "fedora.apim.update");


        String fedoraLocation = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.fedora.location", "http://localhost7880/fedora");


        String fedoraUser = ConfigCollection.getProperties().getProperty(
                "dk.statsbiblioteket.doms.updatetracker.fedora.user", "fedoraReadOnlyAdmin");

        String fedoraPass = ConfigCollection.getProperties().getProperty(
                        "dk.statsbiblioteket.doms.updatetracker.fedora.password", "FedoraReadOnlyPass");

        Credentials creds = new Credentials(fedoraUser, fedoraPass);

        //Start up the fedora connection
        fedora = new Fedora(creds,fedoraLocation);

        //Start up the database
        store = new UpdateTrackerPersistentStoreImpl(fedora);
        try {
            store.setUp();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            store.close();
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
        Connection connection;
        Destination destination;

        // Create the connection.
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(fedoraUser, fedoraPass, jmsurl);
        connection = connectionFactory.createConnection();
        connection.start();

        // Create the session
        Session session = connection.createSession(false,
                                                   Session.AUTO_ACKNOWLEDGE);

        destination = session.createTopic(jmssubject);

        // Create the Consumer
        return session.createConsumer(destination);

    }
}
