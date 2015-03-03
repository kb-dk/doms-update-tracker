package dk.statsbiblioteket.doms.updatetracker.improved;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.EntryAngleCache;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
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
    private static FedoraForUpdateTracker fedora;
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
        EntryAngleCache cmCache = new EntryAngleCache();
        Client client = Client.create();
        WebResource restApi = client.resource(fedoraLocation + "/objects/");
        restApi.addFilter(new HTTPBasicAuthFilter(creds.getUsername(), creds.getPassword()));
        FedoraRest fedoraRest = new FedoraRest(creds, fedoraLocation);
        TripleStoreRest tripleStoreRest = new TripleStoreRest(creds, fedoraLocation, fedoraRest);
        ViewsImpl views = new ViewsImpl(tripleStoreRest, fedoraRest);


        //Start up the fedora connection
        fedora = new FedoraForUpdateTracker(cmCache,fedoraRest,tripleStoreRest,views);

        //Start up the database
        store = new UpdateTrackerPersistentStoreImpl(fedora);

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
