package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.EntryAngleCache;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.fedoraLog.DatabaseLogRetriever;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import javax.jms.JMSException;
import java.net.MalformedURLException;
import java.util.Timer;

/**
 * This is the system that starts the persistent store and the jms listener and ties them together
 */
public class UpdateTrackingSystem implements AutoCloseable{


    private  UpdateTrackerPersistentStore store;
    private  DatabaseLogRetriever consumer;
    private  Timer timer;


    private static UpdateTrackingSystem instance = null;

    protected UpdateTrackingSystem(UpdateTrackingConfig updateTrackingConfig) {


        try {
            Credentials creds = new Credentials(updateTrackingConfig.getFedoraUser(),
                                                updateTrackingConfig.getFedoraPass());
            EntryAngleCache cmCache = new EntryAngleCache();
            FedoraRest fedoraRest = new FedoraRest(creds, updateTrackingConfig.getFedoraLocation());
            TripleStoreRest tripleStoreRest = new TripleStoreRest(creds,
                                                                  updateTrackingConfig.getFedoraLocation(), fedoraRest);
            ViewsImpl views = new ViewsImpl(tripleStoreRest, fedoraRest);


            //Start up the fedora connection
            FedoraForUpdateTracker fedora = new FedoraForUpdateTracker(cmCache, fedoraRest, tripleStoreRest, views);

            //Start up the database
            store = new UpdateTrackerPersistentStoreImpl(updateTrackingConfig.getHibernateConfigFile(), fedora);


            //initialise the jms connection to Fedora
            consumer = new DatabaseLogRetriever(updateTrackingConfig.getFedoraDatabaseDriver(), updateTrackingConfig.getFedoraDatabaseUrl(),
                                                updateTrackingConfig.getFedoraDatabaseUser(),
                                                updateTrackingConfig.getFedoraDatabasePassword());

            final boolean isDaemon = false;
            timer = new Timer(isDaemon);
            //Tie it all together
            final int delay = updateTrackingConfig.getDelay();
            final int period = updateTrackingConfig.getPeriod();
            final int limit = updateTrackingConfig.getLimit();
            timer.schedule(new FedoraMessageListener(consumer, store, limit),
                           delay,
                           period);
        } catch (Exception e){
            close();
            throw new RuntimeException(e);
        }
    }

    public synchronized static UpdateTrackingSystem getInstance(UpdateTrackingConfig updateTrackingConfig) {
        if (instance == null) {
            instance = new UpdateTrackingSystem(updateTrackingConfig);
        }
        return instance;

    }

    @Override
    public void close()  {
        if (timer != null) {
            timer.cancel();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (store != null) {
            store.close();
        }
    }

    public UpdateTrackerPersistentStore getStore() {
        return store;
    }
}
